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
python 07_create_hive_traces_survey.py \
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
        requests STRING,
        r_count INT
    )
    PARTITIONED BY (year INT, month INT, day INT, host STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    """.format(db_name, table_name, lang)

    exec_hive_stat2(query, priority=priority, nice=nice)


def add_day_to_hive_trace_table(req_table, db_name, table_name, day, lang, priority, nice):
    year = day.year
    month = day.month
    day = day.day

    query = """
    INSERT OVERWRITE TABLE {0}.{1}_{2}_by_day
    PARTITION(year={3}, month={4}, day={5}, host='{2}')
    SELECT
        userhash,
        geocoded_data,
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(request)) AS requests,
        count(*) as r_count
    FROM
        (SELECT
            userhash,
            geocoded_data,
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
            {6} w
        WHERE 
            day = {5}
        ) a
    GROUP BY
        userhash,
        geocoded_data;""".format(db_name, table_name, lang, year, month, day, req_table)
#    HAVING
#        COUNT(*) < 500;"""

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
        CONCAT_WS('REQUEST_DELIM', COLLECT_LIST(requests)) AS requests,
        sum(r_count) as request_count,
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

def traces_to_csv(db, table, lang, srv_dir):
    full_tablename = db + "." + table + "_" + lang
    query = "SELECT userhash, geocoded_data, requests from {0};".format(full_tablename)
    exec_hive_stat2(query, os.path.join(srv_dir, "sample_{0}.csv".format(lang)))


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
        '--db', default=config.hive_db,
        help='hive db'
    )

    aparser.add_argument(
        '--name',
        default=config.hive_survey_traces_table,
        help='hive table'
    )

    aparser.add_argument(
        '--priority', default=False, action="store_true",
        help='hive table'
    )

    aparser.add_argument(
        '--nice', default=False, action="store_true",
        help='hive table'
    )

    aparser.add_argument(
        '--lang', nargs='+',
        default=config.languages,
        help='list of languages'
    )

    aparser.add_argument(
        '--req_table', default=config.hive_survey_requests_table,
        help="Hive survey webrequests table"
    )

    args = aparser.parse_args()
    start = args.start
    stop = args.stop
    if isinstance(start, str):
        start = parser.parse(start)
    if isinstance(stop, str):
        stop = parser.parse(stop)

    days = [day for day in pd.date_range(start, stop)]

    for l in args.lang:
        create_hive_trace_table(args.db, args.name, l, priority=args.priority, nice=args.nice)

        for day in days:
            print('Adding Traces From: ', day)
            add_day_to_hive_trace_table(args.req_table, args.db, args.name, day, l, priority=args.priority, nice=args.nice)
        ungroup(args.db, args.name, l, priority=args.priority, nice=args.nice)
        traces_to_csv(args.db, args.name, l, config.srvy_req_folder)
