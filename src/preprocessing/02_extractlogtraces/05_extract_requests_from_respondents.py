import argparse
import os

# hacky way to make sure utils is visible
import sys
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import exec_hive_stat2
from src.utils import config

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all_ids_csv",
                        default=config.all_ids_csv,
                        help="CSV with userIDs from all languages")
    parser.add_argument("--ids_table_name",
                        default=config.hive_ids_table,
                        help="Hive table with hashed userIDs.")
    parser.add_argument("--srvy_req_table",
                        default=config.hive_survey_requests_table,
                        help="Hive table w/ all survey requests")
    parser.add_argument("--all_req_table",
                        default=config.hive_all_requests_table,
                        help="Hive table w/ all webrequests.")
    args = parser.parse_args()


    exec_hive_stat2("DROP TABLE IF EXISTS {0};".format(args.ids_table_name))
    exec_hive_stat2("CREATE TABLE {0} (id string) "
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES "
                    "('separatorChar' = ',', 'quoteChar' = '\\\"');".format(args.ids_table_name))
    exec_hive_stat2("LOAD DATA LOCAL INPATH '{0}' OVERWRITE INTO TABLE {1};".format(args.all_ids_csv, args.ids_table_name))

    query = ("CREATE TABLE {0} STORED AS PARQUET AS "
             "SELECT * FROM {1} "
             "WHERE {1}.id in (SELECT {2}.id from {2});".format(
        args.srvy_req_table, args.all_req_table, args.ids_table_name))
    exec_hive_stat2(query)


if __name__ == "__main__":
    main()
