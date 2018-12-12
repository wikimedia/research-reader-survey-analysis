import argparse
import datetime
import os
import pickle
# hacky way to make sure utils is visible
import sys
import time
from collections import Counter

import numpy as np
import pandas as pd
import pytz

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config


def sessionize(trace, interval=60):
    """Break trace into separate sessions whenever there is interval 60 minute gap between requests."""
    sessions = []
    if len(trace) < 1:
        print("No trace:", trace)
    else:
        session = [trace[0]]
        for r in trace[1:]:
            curr_ts = r['ts']
            prev_ts = session[-1]['ts']
            d = curr_ts - prev_ts
            if d > datetime.timedelta(minutes=interval):
                sessions.append(session)
                session = [r, ]
            else:
                session.append(r)

        sessions.append(session)
    return sessions


def survey_session(df):
    """Identify the session containing the survey request."""
    sessions = df["sessions"]
    survey_request = df["survey_request"]
    survey_request_id = survey_request['id']
    for s in sessions:
        if survey_request_id in [r['id'] for r in s]:
            return [r for r in s if 'is_pageview' in r and r['is_pageview'] == 'true']


def num_sessions(sessions):
    """Number of sessions across the survey time-period"""
    return len(sessions)


def single_surveyation(m):
    """Return motivation if only one provided."""
    if len(m.split('|')) == 1:
        return m
    return None


def get_survey_local_time(dt, timezone):
    """Convert UTC timestamp to local time based on webrequest timezone (based on IP)"""
    utc_dt = pytz.utc.localize(dt, is_dst=None)
    try:
        return utc_dt.astimezone(pytz.timezone(timezone))
    except:
        return None


def get_survey_local_time_hour(dt):
    """Get hour of day when survey was taken as feature."""
    try:
        return dt.hour
    except:
        return None


def get_survey_local_time_dayofweek(dt):
    """Get day of week when survey was taken."""
    try:
        weekday = dt.weekday()
        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return weekdays[weekday]
    except:
        return None


def session_length(session):
    """Number of pageviews in session."""
    return len(session)


def session_time_length(session):
    """Length of time of session."""
    if len(session) < 2:
        return 0
    else:
        start = session[0]['ts']
        end = session[-1]['ts']

        # convert to unix timestamp
        start_ts = time.mktime(start.timetuple())
        end_ts = time.mktime(end.timetuple())

        # they are now in seconds, subtract and then divide by 60 to get minutes.
        return (end_ts - start_ts) / 60.


def session_avg_time_diff(session):
    """Average time between pageviews during session."""
    if len(session) < 2:
        return None
    else:
        time_diffs = []
        for i in range(len(session) - 1):
            curr_pv = session[i]['ts']
            next_pv = session[i + 1]['ts']
            curr_ts = time.mktime(curr_pv.timetuple())
            next_ts = time.mktime(next_pv.timetuple())
            time_diffs.append((next_ts - curr_ts) / 60.)
        return np.average(np.array(time_diffs))


def session_num_article(df):
    """Number of times article with survey request was viewed in the session."""
    session = df.session
    survey_request = df.survey_request
    num = 0
    for r in session:
        if r['title'] == survey_request['title']:
            num += 1
    return num


def session_position(df):
    """Proportion of way through session in which survey was clicked."""
    for i, r in enumerate(df.session):
        if r["id"] == df.survey_request["id"]:
            if len(df.session) == 1.:
                return None
            else:
                return i / (len(df.session) - 1)


def session_access_method(session):
    """Most common access method in session (categorical)."""
    access_method = Counter([r['access_method'] for r in session]).most_common(1)[0][0]
    return access_method


def session_referer_class(session):
    """Most common referer class in session (categorical)."""
    referer_class = Counter([r['referer_class'] for r in session]).most_common(1)[0][0]
    return referer_class


def session_num_pageviews(session):
    """Number of pageviews in session."""
    return len([r for r in session if 'is_pageview' in r and r['is_pageview'] == 'true'])


def session_num_access_methods(session, access_method):
    """Number of times a given access method is associated with a pageview in session."""
    return len([r for r in session if 'referer_class' in r and r['referer_class'] == access_method])


def requests_length(requests):
    """Total number of pageviews across all sessions."""
    return len([r for r in requests if 'is_pageview' in r and r['is_pageview'] == 'true'])


def generate_survey_features(df):
    """Generate features related to context for accessing survey."""
    print("generating survey features")
    if 'motivation' in df:
        df['single_motivation'] = df['motivation'].apply(single_surveyation)

        dummies = df["motivation"].str.get_dummies(sep='|')
        for col in dummies:
            df["motivation_" + col] = dummies[col]

    df['local_time'] = df.apply(lambda x: get_survey_local_time(x['survey_dt_utc'], x['geo_data']['timezone']), axis=1)
    df['local_time_hour'] = df['local_time'].apply(get_survey_local_time_hour)
    df['local_time_weekday'] = df['local_time'].apply(get_survey_local_time_dayofweek)
    df['continent'] = df['geo_data'].apply(lambda x: x['continent'])
    df['country_code'] = df['geo_data'].apply(lambda x: x['country_code'])
    # replace all countries with less than 500 rows ==> 'other'
    value_counts = df['country_code'].value_counts()
    to_remove = value_counts[value_counts <= 500].index
    df['country_code'].replace(to_remove, "other", inplace=True)

    df['host'] = df['survey_request'].apply(lambda x: x['access_method'] if 'access_method' in x else "-")
    # df['access_method'] = df['survey_request'].apply(lambda x: x['access_method'])
    # df['is_pageview'] = df['survey_request'].apply(lambda x: x['is_pageview'])
    df['referer_class'] = df['survey_request'].apply(lambda x: x['referer_class'] if 'referer_class' in x else "-")

    df = df.rename(columns={'survey_title': 'article_title'})

    return df


def generate_session_features(df):
    """Generate features about the specific survey session of pageviews."""
    print("generating survey session features")
    df['sessions'] = df['requests'].apply(sessionize)
    df['num_sessions'] = df['sessions'].apply(num_sessions)
    # df['session'] = df[['sessions','survey_request']].apply(survey_session,axis=1)
    # replace the above with a loop due to: https://github.com/pandas-dev/pandas/issues/14217
    session_list = []
    for _, l in df.iterrows():
        session_list.append(survey_session(l))
    df["session"] = session_list

    df['session_length'] = df['session'].apply(session_length)
    df['session_time_length'] = df['session'].apply(session_time_length)
    df['session_rel_position'] = df.apply(session_position, axis=1)
    # df['session_access_method'] = df['session'].apply(session_access_method)
    df["session_avg_time_diff"] = df['session'].apply(session_avg_time_diff)
    df["session_num_article"] = df[['session', 'survey_request']].apply(session_num_article, axis=1)
    # df["session_num_pageviews"] = df['session'].apply(session_num_pageviews)
    # df["session_avg_num_pageviews"] = df["session_num_pageviews"] / df['session_length']
    # df['session_referer_class'] = df['session'].apply(session_referer_class)
    df["session_num_external_searches"] = df['session'].apply(
        lambda x: session_num_access_methods(x, 'external (search engine)'))
    # df["session_avg_num_external_searches"] = df["session_num_external_searches"] / df['session_length']
    df["session_num_internal"] = df['session'].apply(lambda x: session_num_access_methods(x, "internal"))
    # df["session_avg_num_internal"] = df["session_num_internal"] / df['session_length']
    df["session_num_external_nonsearches"] = df['session'].apply(lambda x: session_num_access_methods(x, "external"))
    # df["session_avg_external_nonsearches"] = df["session_num_external_nonsearches"] / df['session_length']
    df["session_num_noreferer"] = df['session'].apply(lambda x: session_num_access_methods(x, "none"))
    # df["session_avg_noreferer"] = df["session_num_noreferer"] / df['session_length']

    return df


def generate_request_features(df):
    """Features summarizing all requests across all sessions."""
    print("generating request features")
    df['requests_length'] = df['requests'].apply(requests_length)
    return df


def delete_columns(df):
    """Trim down data."""
    del df["sessions"]
    del df["requests"]
    del df["session"]
    del df["session_articles"]
    del df["survey_request"]
    del df["ua_data"]
    del df["geo_data"]
    del df["client_token"]
    del df["monthly_pageviews"]
    del df["local_time"]

    if 'motivation' in df:
        del df["survey_submit_dt"]
        del df["raw_information_depth"]
        del df["raw_prior_knowledge"]
        del df["survey_time_to_click"]
        del df["survey_dt_utc"]
        del df["survey_token"]
        del df["raw_motivation"]


def topic_entropy(topics):
    """Compute topic entropy -- i.e. how specific / general the article topic was."""
    entropy = 0
    for p in topics:
        entropy += -p * np.log2(p)
    return entropy


def generate_article_features(df, df_articles):
    """Generate features specific to the article on which the survey was responded."""
    print("generating article features")

    df["article_title"] = df["survey_request"].apply(lambda x: x["title"].lower())
    df_all = pd.merge(left=df, right=df_articles, left_on="article_title", right_on="title", how="left")

    topics = ["topic_{0}".format(x) for x in range(20)]
    df_all["topic_entropy"] = df_all[topics].apply(topic_entropy, axis=1)
    return df_all


def generate_article_session_features(df, df_a):
    """Generate features about the articles viewed across all sessions."""
    print("generate article session features")
    df["session_avg_pagerank_difference"] = [session_article_pagerank_diff_avg(x, df_a) for x in df["session"]]
    df["session_avg_topic_distance"] = [session_topic_difference_avg(x, df_a) for x in df["session"]]
    return df


def session_article_pagerank_diff_avg(session, df_articles):
    """Average change in pagerank across the entire session.

    NOTE: this is based on the first and last articles viewed, not
    the actual trajectory of pageviews.
    """
    if len(session) < 2:
        return np.nan

    first_title = session[0]["title"].lower()
    pr_first = df_articles.loc[first_title]["pagerank"]
    last_title = session[-1]["title"].lower()
    pr_last = df_articles.loc[last_title]["pagerank"]

    return (pr_first - pr_last) / (len(session) - 1)


def session_topic_difference_avg(ses, df_articles):
    """Average magnitude of difference between topics between each pageview.

    High => session that involves jumping around many different topics
    Low => session that involves staying in the same topic area
    """
    topics = ["topic_{0}".format(x) for x in range(20)]
    if len(ses) < 2:
        return np.nan

    all_diffs = []
    curr_title = None
    for r in ses:
        title = r["title"].lower()
        if curr_title == None:
            curr_title = title
            curr_topics = df_articles.loc[curr_title][topics].values
        else:
            prev_topics = curr_topics
            curr_title = title
            curr_topics = df_articles.loc[curr_title][topics].values
            diff = np.abs(prev_topics - curr_topics)
            all_diffs.append(np.sum(diff))
    return np.mean(all_diffs)


def select_and_rename(df, survey):
    """Cleaning: rename column titles."""
    df = df.rename(columns={"indegree": "article_indegree",
                            "outdegree": "article_outdegree",
                            "pagerank": "article_pagerank",
                            "page_lengths": "article_textlength"})

    df = df.rename(columns={f"topic_{i}": f"t{i}" for i in range(20)})
    features = ['host',
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

    if survey:
        features.extend([
            "information depth",
            "prior knowledge",
            "motivation",
            "single_motivation",
            "motivation_bored/random",
            "motivation_conversation",
            "motivation_current_event",
            "motivation_intrinsic_learning",
            "motivation_media",
            "motivation_other",
            "motivation_personal_decision",
            "motivation_work/school"])
    df = df[features]

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--languages",
                        nargs="*",
                        default=config.languages,
                        help="List of languages to process.")
    parser.add_argument("--response_dir",
                        default=config.srvy_anon_folder,
                        help="Folder with joined responses / traces.")
    parser.add_argument("--featuredf_dir",
                        default=config.features_folder,
                        help="Folder for pickled feature DFs.")
    parser.add_argument("--sample_dir",
                        default=config.smpl_anon_folder,
                        help="Folder with control sample traces.")
    parser.add_argument("--article_folder",
                        default=config.article_folder,
                        help="Folder with article-specific features")
    args = parser.parse_args()

    ## create feature dataframe for survey participants
    for lang in args.languages:
        print("*****************")
        print("* Creating survey features: ", lang)
        df = pd.read_pickle(os.path.join(args.response_dir, 'joined_responses_and_traces_anon_{0}.p'.format(lang)))
        print("Length of df:", len(df))
        df = generate_survey_features(df)
        df = generate_session_features(df)
        df = generate_request_features(df)
        df_articles = pickle.load(open(os.path.join(args.article_folder, "article_features_{0}.p".format(lang)), "rb"))
        df = generate_article_features(df, df_articles)
        df_articles.set_index("title", inplace=True)
        df = generate_article_session_features(df, df_articles)
        df = select_and_rename(df, True)
        df.to_pickle(os.path.join(args.featuredf_dir, 'survey_features_{0}.p'.format(lang)))

    ## create feature dataframe for random sample
    for lang in args.languages:
        print("*****************")
        print("* Creating sample features: ", lang)
        df = pd.read_pickle(os.path.join(args.sample_dir, "samples_anon_{0}.p".format(lang)))
        print("Length of df:", len(df))
        df = generate_survey_features(df)
        df = generate_session_features(df)
        df = generate_request_features(df)
        df_articles = pickle.load(open(os.path.join(args.article_folder, "article_features_{0}.p".format(lang)), "rb"))
        df = generate_article_features(df, df_articles)
        df_articles.set_index("title", inplace=True)
        df = generate_article_session_features(df, df_articles)
        df = select_and_rename(df, False)
        df.to_pickle(os.path.join(args.featuredf_folder, 'sample_features_{0}.p'.format(lang)))


if __name__ == "__main__":
    main()
