import os

from dateutil import parser

# top-level folder for all data
data_folder = "/home/isaacj/demographics/data"

quicksurvey_requests_tsv = os.path.join(data_folder, "survey-requests.tsv")
quicksurvey_el_tsv = os.path.join(data_folder, "el_responses.tsv")
edit_el_tsv = os.path.join(data_folder, "el_editattempts.tsv")

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

# folder containing page graph data
article_graph_folder = os.path.join(data_folder, "graph")

# folder containing features for input into debiasing model
features_folder = os.path.join(data_folder, "feature_dfs")

# folder containing article titles
titles_folder = os.path.join(data_folder, "titles")

# folder containing page details (page.sql.gz dump files)
page_sql_folder = os.path.join(data_folder, 'pages.sql')
page_data_folder = os.path.join(data_folder, 'text')
page_txt_folder = os.path.join(page_data_folder, 'raw')
sql_date = "20181201"
num_lda_topics = 20
lda_max_df = 0.3  # exclude terms found in 30% of articles
lda_min_df = 3  # terms must appear in at least k articles
lda_max_features = 100000 # keep only top k terms


# pageviews folder
pageviews_folder = os.path.join(data_folder, "pageviews")

# folder containing final survey responses with weights
weighted_response_dir = os.path.join(data_folder, "weighted_responses")

# folder containing subgroup Python module
module_path = '/home/flemmerich/pysubgroup'

# list of features to include
featurelist_numerical = os.path.join(data_folder, 'featurelist_numerical.p')
featurelist_categorical = os.path.join(data_folder, 'featurelist_categorical.p')

# top-level folder for all results
results_folder = "/home/isaacj/demographics/results"

# subgroup results folder
sg_folder = os.path.join(results_folder, "sg")

# Hive DB w/ subsampled requests
hive_db = 'isaacj'
survey_name_start = 'reader-demographics'
hive_el_requests_table = '{0}.el_requests'.format(hive_db)
hive_ids_table = "{0}.survey_ids".format(hive_db)
hive_survey_requests_table = "{0}.survey_requests".format(hive_db)
hive_all_requests_table = "{0}.all_requests_parquet".format(hive_db)
hive_survey_traces_table = 'survey_traces'
hive_sample_traces_table = 'sample_traces'

# MariaDB with EventLogging data
quicksurvey_requests_db = 'log'
quicksurvey_requests_table = 'QuickSurveysResponses_18397510'

# additional key for salting IP / user-agent hash
# NOTE: change this to something analysis-specific
with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'hash_key.txt'), 'r') as fin:
    hash_key = fin.read().strip()

# Request delimiter for session strings
request_delim = 'REQUEST_DELIM'

# list of languages in which surveys were conducted
languages = ['en']#["ar", "bn", "de","es", "he", "hi", "hu", "ja", "nl", "ro", "ru", "uk", "zh", "en"]
main_page_titles = {'ar': 'الصفحة_الرئيسية',
                    'bn': 'প্রধান_পাতা',
                    'de': 'Wikipedia:Hauptseite',
                    'en': 'Main_Page',
                    'es': 'Wikipedia:Portada',
                    'he': 'עמוד_ראשי',
                    'hi': 'मुखपृष्ठ',
                    'hu': 'Kezdőlap',
                    'ja': 'メインページ',
                    'nl': 'Hoofdpagina',
                    'ro': 'Pagina_principală',
                    'ru': 'Заглавная_страница',
                    'uk': 'Головна_сторінка',
                    'zh': 'Wikipedia:首页'}

# key dates [includes start, excludes end)
survey_start_date = parser.parse("2019-03-04")
survey_end_date = parser.parse("2019-03-06")
hive_days_clause = " year = 2019 AND month = 3 AND day >= 4 and day < 6 "  # I would love to generate this automatically but it gets painful
mariadb_days_clause = "timestamp > {0} AND timestamp < {1}".format(
    survey_start_date.strftime("%Y%m%d%H%M%S"), survey_end_date.strftime("%Y%m%d%H%M%S"))
