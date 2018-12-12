import argparse
import os
import sys

# hacky way to make sure utils is visible
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config
from src.utils import exec_hive_stat2
from src.utils import hash_key

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_csv",
                        default=config.quicksurvey_requests_tsv,
                        help="CSV filename for output survey-related webrequests")
    parser.add_argument("--quicksurvey_requests_table",
                        default=config.hive_el_requests_table,
                        help="Hive table with all potential QuickSurvey webrequests and surveySessionTokens")
    args = parser.parse_args()

    # All Hive webrequests including QuickSurvey beacon (survey may have run) on the days while survey was live
    get_qs_query = ("CREATE TABLE {0} AS "
                    'SELECT *, get_json_object(json_event, "$.event.surveySessionToken") AS survey_session_token '
                    "FROM ( "
                        'SELECT *, reflect("java.net.URLDecoder", "decode", substr(uri_query, 2)) AS json_event '
                        "FROM wmf.webrequest "
                        'WHERE uri_path LIKE "%beacon/event" AND uri_query LIKE "%QuickSurvey%" AND '
                        "{1}"
                    ") q1;".format(args.quicksurvey_requests_table, config.hive_days_clause))
    exec_hive_stat2(get_qs_query)


    anonymized_to_csv_query = ("SELECT dt, "
                               "reflect('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', concat(client_ip, user_agent, '{0}')) as id, "
                               "survey_session_token "
                               'WHERE client_ip <> "-" AND user_agent <> "-" '
                               "FROM {1};".format(hash_key, args.quicksurvey_requests_table))

    exec_hive_stat2(anonymized_to_csv_query, args.output_csv)

if __name__ == "__main__":
    main()