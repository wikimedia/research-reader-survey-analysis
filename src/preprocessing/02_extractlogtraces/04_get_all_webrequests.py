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
    args = parser.parse_args()

    query = ("CREATE TABLE motivations.all_requests_parquet STORED AS PARQUET AS "
             "SELECT reflect('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', concat(client_ip, user_agent, '{0}')) as id,"
             "geocoded_data, "
             "http_status, "
             "accept_language, "
             "x_forwarded_for, "
             "ts, "
             "referer, "
             "uri_path, "
             "uri_host, "
             "uri_query, "
             "is_pageview, "
             "access_method, "
             "referer_class, "
             "normalized_host, "
             "pageview_info, "
             "page_id, "
             "namespace_id, "
             "day, "
             "hour"
             "FROM wmf.webrequest "
             "WHERE {1} "
             "AND webrequest_source = 'text' AND access_method != 'mobile app' AND agent_type = 'user' "
             "AND namespace_id = 0 and is_pageview = TRUE;".format(args.hash_key, config.hive_days_clause))

    exec_hive_stat2(query)

if __name__ == "__main__":
    main()