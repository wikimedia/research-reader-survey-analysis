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
                    "SELECT *, reflect('java.net.URLDecoder', 'decode', substr(uri_query, 2)) AS json_event "
                    "FROM wmf.webrequest "
                    "WHERE uri_path LIKE '%beacon/event' AND uri_query LIKE '%QuickSurvey%' AND uri_query LIKE '%{1}%' "
                    "AND {2}".format(args.quicksurvey_requests_table, config.survey_name_start, config.hive_days_clause))
    exec_hive_stat2(get_qs_query)

    # NOTE: empirically, the client_ip and user_agent checks have filtered out zero webrequests
    anonymized_to_csv_query = ("SELECT dt as dt_QSinitialization, "
                               "reflect('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', concat(client_ip, user_agent, '{0}')) as userhash, "
                               "get_json_object(json_event, '$.event.surveySessionToken') AS survey_session_token, "
                               "get_json_object(json_event, '$.event.pageviewToken') as pageview_token, "
                               "get_json_object(json_event, '$.event.surveyResponseValue') as response_type, "
                               "get_json_object(json_event, '$.event.pageTitle') as page_title, "
                               "get_json_object(json_event, '$.event.pageId') as page_id, "
                               "get_json_object(json_event, '$.event.isLoggedIn') as logged_in, "
                               "geocoded_data['country'] as country, "
                               "geocoded_data['timezone'] as timezone "
                               "FROM {1} "
                               "WHERE client_ip <> '-' AND "
                               "user_agent <> '-'".format(hash_key, args.quicksurvey_requests_table))

    exec_hive_stat2(anonymized_to_csv_query, args.output_csv)

if __name__ == "__main__":
    main()