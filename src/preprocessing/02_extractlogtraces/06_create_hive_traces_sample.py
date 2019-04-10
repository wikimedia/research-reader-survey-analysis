import argparse
from dateutil import parser
from dateutil import relativedelta
import os

import pandas as pd

# hacky way to make sure utils is visible
import sys
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config
from src.utils import exec_hive_stat2

"""
USAGE:
python create_hive_traces.py \
--start 2016-05-01 \
--stop 2016-05-01 \
--db traces \
--name test \
--priority
"""


def get_hive_timespan_mod(start, stop, hour=False):
    days = []
    while start <= stop:
        parts = []
        parts.append('day=%d' % start.day)
        if hour:
            parts.append('hour=%d' % start.hour)
            start += relativedelta.relativedelta(hours=1)
        else:
            start += relativedelta.relativedelta(days=1)
        days.append(' AND '.join(parts))

    condition = '((' + (') OR (').join(days) + '))'
    return condition


def create_hive_trace_table(db_name, table_name, lang, priority, nice):
    """
    Create a Table partitioned by day and host
    """

    query = """
    CREATE TABLE IF NOT EXISTS {0}.{1}_{2}_by_day (
        userhash STRING,
        geocoded_data MAP<STRING,STRING>,
        logged_in INT,
        requests STRING,
        r_count INT
    )
    PARTITIONED BY (year INT, month INT, day INT, host STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    """.format(db_name, table_name, lang)

    exec_hive_stat2(query, priority=priority, nice=nice)


def add_day_to_hive_trace_table(req_table, db_name, table_name, day, lang, priority, nice, sampling_rate=1.0):
    year = day.year
    month = day.month
    day = day.day

    query = """
    INSERT OVERWRITE TABLE {0}.{1}_{2}_by_day
    PARTITION(year={3}, month={4}, day ={5}, host='{2}')
    SELECT
        userhash,
        geocoded_data,
        MAX(logged_in) as logged_in,
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(request)) AS requests,
        COUNT(*) as r_count
    FROM
        (SELECT
            userhash,
            geocoded_data,
            logged_in,
            cast(normalized_host.project = '{2}' as int) as correct_wiki,
            CONCAT( 'ts|', ts,
                    '|referer|', referer,
                    '|page_id|', page_id,
                    '|title|', pageview_info['page_title'],
                    '|uri_path|', reflect('java.net.URLDecoder', 'decode', uri_path),
                    '|uri_query|', reflect('java.net.URLDecoder', 'decode', uri_query),
                    '|access_method|', access_method,
                    '|referer_class|', referer_class,
                    '|project|', normalized_host.project_class,
                    '|lang|', normalized_host.project,
                    '|uri_host|', uri_host
                ) AS request
        FROM
            {6}
        WHERE 
            day = {5}
            AND CONV(SUBSTR(userhash, 113), 16, 10) / 18446744073709551615 < {7}
        ) a
    GROUP BY
        userhash,
        geocoded_data
    HAVING
    COUNT(*) < 500 AND SUM(correct_wiki) > 0;
    """.format(db_name, table_name, lang, year, month, day, req_table, sampling_rate)

    exec_hive_stat2(query, priority=priority, nice=nice)


def ungroup(db_name, table_name, lang, priority, nice, year=config.survey_start_date.year):
    query = """
    CREATE TABLE {0}.{1}_{2}
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET AS
    SELECT
        userhash,
        geocoded_data,
        MAX(logged_in) as has_account,
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(requests)) AS requests,
        SUM(r_count) as request_count,
        RAND() AS rand_sample
    FROM
        {0}.{1}_{2}_by_day
    WHERE
        year = {3}
    GROUP BY
        userhash,
        geocoded_data
    """.format(db_name, table_name, lang, year)

    exec_hive_stat2(query, priority=priority, nice=nice)

def traces_to_csv(db, table, lang, smpl_req_folder, max_num=200000):
    full_tablename = db + "." + table + "_" + lang
    query = ("SET mapreduce.map.memory.mb=9000; "
             "SET mapreduce.map.java.opts=-Xmx7200m; "
             "SET mapreduce.reduce.memory.mb=9000; "
             "SET mapreduce.reduce.java.opts=-Xmx7200m; "
             "SELECT userhash, geocoded_data, requests "
             "FROM ("
             "SELECT * "
             "FROM {0} "
             "WHERE request_count < 500 "
             "ORDER BY rand_sample "
             "LIMIT {1}) w;".format(full_tablename, max_num))

    exec_hive_stat2(query, os.path.join(smpl_req_folder, "sample_{0}.csv".format(lang)))


if __name__ == '__main__':

    aparser = argparse.ArgumentParser()
    aparser.add_argument(
        '--start',
        default=config.survey_start_date,
        help='start day'
    )

    aparser.add_argument(
        '--stop',
        default=config.survey_end_date,
        help='start day'
    )

    aparser.add_argument(
        '--db',
        default=config.hive_db,
        help='hive db'
    )

    aparser.add_argument(
        '--name',
        default=config.hive_sample_traces_table,
        help='hive table'
    )

    aparser.add_argument(
        '--rate', required=True,
        help='sampling_rate'
    )
    aparser.add_argument(
        '--priority', action="store_true",
        default=False,
        help='prioritize query'
    )

    aparser.add_argument(
        '--nice', action="store_true",
        default=False,
        help='deprioritize query'
    )

    aparser.add_argument(
        '--lang', nargs='+',
        default=config.languages,
        help='list of languages'
    )

    aparser.add_argument(
        '--req_table',
        default=config.hive_all_requests_table,
        help="Hive survey webrequests table"
    )

    aparser.add_argument(
        '--max_samples', type=int,
        default=200000,
        help="Maximum # of samples to keep."
    )


    args = aparser.parse_args()
    start = args.start
    stop = args.stop
    if isinstance(start, str):
        start = parser.parse(start)
    if isinstance(stop, str):
        stop = parser.parse(stop)
    days = [day for day in pd.date_range(start, stop)]

    if not os.path.isdir(config.smpl_req_folder):
        print("Creating directory: {0}".format(os.path.abspath(config.smpl_req_folder)))
        os.mkdir(config.smpl_req_folder)

    for l in args.lang:
        create_hive_trace_table(args.db, args.name, l, priority=args.priority, nice=args.nice)

        for day in days:
            print('Adding Traces From: ', day)
            add_day_to_hive_trace_table(args.req_table, args.db, args.name, day, l, sampling_rate=args.rate,
                                        priority=args.priority, nice=args.nice)
        ungroup(args.db, args.name, l, priority=args.priority, nice=args.nice)
        traces_to_csv(args.db, args.name, l, config.smpl_req_folder, max_num=args.max_samples)