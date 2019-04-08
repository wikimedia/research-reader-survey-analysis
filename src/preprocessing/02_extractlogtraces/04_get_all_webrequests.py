import argparse
import os
import sys

# hacky way to make sure utils is visible
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config
from src.utils import exec_hive_stat2

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hash_key",
                        default=config.hash_key,
                        help="Hash key for salting user-agent + client-IP")
    parser.add_argument("--all_req_table",
                        default=config.hive_all_requests_table,
                        help="Hive table w/ all webrequests.")
    args = parser.parse_args()

    query = ("CREATE TABLE {0} STORED AS PARQUET AS "
             "SELECT reflect('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', concat(client_ip, user_agent, '{1}')) as userhash,"
             "geocoded_data, "
             "ts, "
             "referer, "
             "uri_path, "
             "uri_host, "
             "uri_query, "
             "access_method, "
             "referer_class, "
             "normalized_host, "
             "pageview_info, "
             "page_id, "
             "day, "
             "hour "
             "FROM wmf.webrequest "
             "WHERE {2} "
             "AND webrequest_source = 'text' AND access_method != 'mobile app' AND agent_type = 'user' "
             "AND normalized_host.project_class = 'wikipedia' "
             "AND namespace_id = 0 and is_pageview = TRUE;".format(args.all_req_table, args.hash_key, config.hive_days_clause))

    exec_hive_stat2(query)

if __name__ == "__main__":
    main()