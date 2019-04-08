import argparse
import os
import sys

# hacky way to make sure utils is visible
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config
from src.utils import exec_hive_stat2

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_tsv",
                        default=config.edit_el_tsv,
                        help="TSV filename for output EventLogging data")
    parser.add_argument("--hive_requests_table",
                        default=config.hive_el_requests_table,
                        help="Hive table with all potential QuickSurvey webrequests and surveySessionTokens")
    args = parser.parse_args()

    # make sure dates WHERE clause matches config logic
    query = ("SELECT event.session_token AS session_token, "
                    "event.action AS action, "
                    "event.init_mechanism AS init_mechanism, "
                    "event.editor_interface AS editor_interface, "
                    "event.page_title AS edit_page_title, "
                    "event.user_editcount AS user_edit, "
                    "event.user_id = 0 AS anon, "
                    "REFLECT('org.apache.commons.codec.digest.DigestUtils', 'sha512Hex', CONCAT(s.client_ip, s.user_agent, '{0}')) AS userhash "
               "FROM event.editattemptstep e "
              "INNER JOIN {1} s "
                 "ON (e.event.session_token = SUBSTR(s.survey_session_token, 0, 20)) "
              "WHERE e.year = 2019 AND e.month = 3 AND (e.day = 4 OR e.day = 5)".format(config.hash_key, args.hive_requests_table))
    exec_hive_stat2(query, args.output_tsv)

if __name__ == "__main__":
    main()