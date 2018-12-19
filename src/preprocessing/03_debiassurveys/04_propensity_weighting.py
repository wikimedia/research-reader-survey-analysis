import argparse
import os
import sys

import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Imputer

# hacky way to make sure utils is visible
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--languages",
                        default=config.languages,
                        nargs="*",
                        help="List of languages to process")
    parser.add_argument("--featuredf_dir",
                        default=config.features_folder,
                        help="Folder containing survey/control features")
    parser.add_argument("--weighted_response_dir",
                        default=config.weighted_response_dir,
                        help="Folder for output, weighted survey responses")
    args = parser.parse_args()

    feature_list = ['host',
                    'local_time_hour',
                    'local_time_weekday',
                    'continent',
                    'country_code',
                    'referer_class',
                    'num_sessions',
                    'session_length',
                    'session_time_length',
                    'session_avg_time_diff',
                    'session_num_article',
                    'session_num_external_searches',
                    'session_num_internal',
                    'session_num_external_nonsearches',
                    'session_num_noreferer',
                    'requests_length',
                    'article_indegree',
                    'article_outdegree',
                    'article_pagerank',
                    'article_textlength',
                    't0',
                    't1',
                    't10',
                    't11',
                    't12',
                    't13',
                    't14',
                    't15',
                    't16',
                    't17',
                    't18',
                    't19',
                    't2',
                    't3',
                    't4',
                    't5',
                    't6',
                    't7',
                    't8',
                    't9',
                    'weekly_pageviews',
                    'topic_entropy',
                    'session_avg_pagerank_difference',
                    'session_avg_topic_distance',
                    'session_rel_position']

    features_categorical = ['continent', 'host', 'referer_class', 'local_time_weekday', 'country_code']

    for lang in args.languages:
        print("*****************")
        print("* Creating weights for: ", lang)
        print("*****************")

        # Load dataframes with the features
        print("loading data")
        df_survey = pd.read_pickle(os.path.join(args.featuredf_dir, 'survey_features_{0}.p'.format(lang)))
        df_sample = pd.read_pickle(os.path.join(args.featuredf_dir, 'sample_features_{0}.p'.format(lang)))
        if len(df_sample) > (len(df_survey) * 10):
            df_sample = df_sample.sample(len(df_survey) * 10, replace=False)

        # Add column that specifies if row comes from survey answer or random sample
        df_survey['fromSurvey'] = True
        df_sample['fromSurvey'] = False

        print("preparing data")
        # put both in a single dataframe
        df = pd.concat([df_survey, df_sample], ignore_index=True)

        X = df[feature_list]
        # transform categorical features into binaries
        X = pd.get_dummies(X, columns=features_categorical, drop_first=True)

        # Prepare target
        Y = df['fromSurvey'].astype(int)

        print("Compute gradient boosting weights")
        model = Pipeline([("imputer", Imputer()),
                          ("clsfr", GradientBoostingClassifier(n_estimators=500, verbose=1))])
        model.fit(X, Y)
        df['weights_gbc'] = 1 / model.predict_proba(X)[:, 1]

        print("Compute logistic regression weights")
        model = Pipeline([("imputer", Imputer()), ("clsfr", LogisticRegression())])
        model.fit(X, Y)
        df['weights_logreg'] = 1 / model.predict_proba(X)[:, 1]

        print("writing weighted file (survey_samples)")
        df = df[df['fromSurvey'] == True]

        df.to_pickle(os.path.join(args.weighted_response_dir, 'weighted_responses_{0}.p'.format(lang)))


if __name__ == "__main__":
    main()
