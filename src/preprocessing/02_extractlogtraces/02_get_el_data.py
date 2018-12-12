import argparse
import os
import sys

# hacky way to make sure utils is visible
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config
from src.utils import exec_mariadb_stat2

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_tsv",
                        default=config.quicksurvey_el_tsv,
                        help="TSV filename for output EventLogging data")
    parser.add_argument("--quicksurvey_requests_db",
                        default=config.quicksurvey_requests_db,
                        help="MariaDB database with EventLogging data")
    parser.add_argument("--quicksurvey_requests_table",
                        default=config.quicksurvey_requests_table,
                        help="MariaDB table with QuickSurvey EL data")
    args = parser.parse_args()

    # EventLogging data associated with the QuickSurveys (language-independent)
    get_el_query = ("SELECT * FROM {0} WHERE {1};".format(args.quicksurvey_requests_table, config.mariadb_days_clause))

    exec_mariadb_stat2(get_el_query, args.quicksurvey_requests_db, args.output_tsv)

if __name__ == "__main__":
    main()