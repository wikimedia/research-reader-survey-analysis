import os
from dateutil import parser

# top-level folder for all data
data_folder = "/home/flemmerich/wikimotifs2/data/"
quicksurvey_requests_tsv = os.path.join(data_folder, "survey-requests.tsv")
quicksurvey_el_tsv = os.path.join(data_folder, "el_responses.tsv")

# folder containing survey responses w/ associated webrequest
responses_folder = os.path.join(data_folder, "responses_with_ip")

# folder containing approximate userIDs for matching against webrequests
ids_folder = os.path.join(data_folder, "ids")
all_ids_csv = os.path.join(ids_folder, "all_ids.csv")

# folder containing survey webrequests
srvy_req_folder = os.path.join(data_folder, "survey_traces")

# folder containing anonymized survey webrequests / responses
srvy_anon_folder = os.path.join(data_folder, "joined_responses_and_traces_anon")

# folder containing sample webrequests
smpl_req_folder = os.path.join(data_folder, "samples")

# folder containing anonymized sample webrequests (as controls against survey)
smpl_anon_folder = os.path.join(data_folder, "samples_anon")

# folder containing data about redirects on Wikipedia
redirect_folder = os.path.join(data_folder, "redirect_tables")

# folder containing article-specific features
article_folder = os.path.join(data_folder, "article_features")

# folder containing features for input into debiasing model
features_folder = os.path.join(data_folder, "feature_dfs")

# Hive DB w/ subsampled requests
hive_db = 'motivations'
hive_el_requests_table = '{0}.el_requests'.format(hive_db)
hive_ids_table = "{0}.survey_ids".format(hive_db)
hive_survey_requests_table = "{0}.survey_requests".format(hive_db)
hive_all_requests_table = "{0}.all_requests_parquet".format(hive_db)


# MariaDB with EventLogging data
quicksurvey_requests_db = 'log'
quicksurvey_requests_table = 'QuickSurveysResponses_15266417'

# additional key for salting IP / user-agent hash
# NOTE: change this to something analysis-specific
hash_key = "KEY"

# list of languages in which surveys were conducted
languages = ["ar", "bn", "de","es", "he", "hi", "hu", "ja", "nl", "ro", "ru", "uk", "zh", "en"]

# key dates [includes start, excludes end)
survey_start_date = parser.parse("2017-06-22")
survey_end_date = parser.parse("2017-06-30")
hive_days_clause = " year = 2017 AND month = 6 AND day >= 22 and day < 30 "  # I would love to generate this automatically but it gets painful
mariadb_days_clause = "timestamp > {0} AND timestamp < {1}".format(
    survey_start_date.strftime("%Y%m%d%H%M%S"), survey_end_date.strftime("%Y%m%d%H%M%S"))
