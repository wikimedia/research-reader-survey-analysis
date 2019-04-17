import argparse
import datetime
import os
import pickle
# hacky way to make sure utils is visible
import sys

import pandas as pd

sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config
from src.utils import read_redirects


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--languages",
                        default=config.languages,
                        nargs="*",
                        help="List of languages to process")
    parser.add_argument("--in_dir_traces",
                        default=config.srvy_req_folder,
                        help="Folder with webrequest traces")
    parser.add_argument("--in_dir_responses",
                        default=config.responses_folder,
                        help="Folder with survey responses")
    parser.add_argument("--redirect_dir",
                        default=config.redirect_folder,
                        help="Folder with Wikipedia redirects")
    parser.add_argument("--out_dir",
                        default=config.srvy_anon_folder,
                        help="Folder for output joined responses/traces.")
    args = parser.parse_args()

    if not os.path.isdir(args.out_dir):
        print("Creating directory: {0}".format(os.path.abspath(args.out_dir)))
        os.mkdir(args.out_dir)


    geo_cols = ["country", "timezone"]
    editattempt_cols = ["edit_count", "editor_interface", "is_anon"]
    columns_to_keep = geo_cols + [
        "requests",
        "submit_timestamp",
        ">=18",
        "raw_information_depth", "raw_prior_knowledge", "raw_motivation",
        "information depth", "prior knowledge", "motivation",
        "age", "gender", "education", "locale", "native_lang_1", "native_lang_2",
        "dt_qsinitialization",
        "response_type",
        "wiki",
        "page_title",
        "page_id",
        "survey_request",
        "survey_dt_utc",
        "has_account", 'attempted_edit']

    for lang in args.languages:
        print("**************")
        print("* Processing " + lang)
        print("**************")
        with open(os.path.join(args.in_dir_traces, "sample_{0}.csv".format(lang)), "r") as f:
            lines = []
            assert next(f).strip().split('\t') == ['userhash', 'geocoded_data', 'has_account', 'attempted_edit', 'requests']
            for l_count, line in enumerate(f, start=1):
                l = parse_row(line)
                if l is not None:
                    lines.append(l)
                if l_count % 10000 == 0:
                    print("processing line...", l_count)
            print("traces processed: ", l_count)

        df_traces = pd.DataFrame(lines)
        print("traces kept", len(df_traces))
        df_traces.drop_duplicates(subset=["userhash"], inplace=True)
        print("traces without duplicates", len(df_traces))

        df_responses = pd.read_csv(os.path.join(args.in_dir_responses, "responses_with_el_{0}.csv".format(lang)),
                                   sep="\t")
        print("responses with duplicates:", len(df_responses))
        df_responses = df_responses[~df_responses['userhash'].isnull()]
        print("responses after removing null userhashes (missing EL):", len(df_responses))
        df_responses.drop_duplicates(subset=["userhash"], inplace=True)
        print("responses after removing remaining duplicates:", len(df_responses))


        df_merged = pd.merge(df_traces, df_responses, left_on=["userhash"],
                             right_on=["userhash"], how="inner")
        print("Users in merged dataframe of survey responses and webrequest traces:", len(df_merged))

        df_merged['requests'] = df_merged['requests'].apply(parse_requests_ts_and_redirects,
                                                            d=read_redirects(lang, args.redirect_dir))
        df_merged['survey_request'] = df_merged.apply(extract_survey_request, lang=lang, axis=1)
        df_merged['wiki'] = df_merged.apply(lambda x: x['survey_request'].get('uri_host', lang), axis=1)
        df_merged['survey_dt_utc'] = df_merged['survey_request'].apply(lambda x: x.get('ts', None))
        df_merged['page_title'] = df_merged['survey_request'].apply(lambda x: x['title'])
        df_merged['page_id'] = df_merged['survey_request'].apply(lambda x: x['page_id'])
        df_merged.dropna(subset=["survey_dt_utc"], inplace=True)
        print("After removing non-existing survey requests: ", len(df_merged))
        df_merged = df_merged.reset_index(drop=True)
        unmatched_countries = df_merged[
            df_merged['geocoded_data'].apply(lambda x: x['country']) != df_merged['country']]
        if len(unmatched_countries) > 0:
            print("Unmatched countries:", unmatched_countries)

        print("Anonymizing survey...")
        for geo_col in geo_cols:
            df_merged[geo_col] = df_merged['geocoded_data'].apply(lambda x: x.get(geo_col, None))
        df_merged = df_merged[columns_to_keep]
        pickle.dump(df_merged,
                    open(os.path.join(args.out_dir, "joined_responses_and_traces_anon_{0}.p".format(lang)), "wb"))
        df_merged.to_csv(os.path.join(args.out_dir, "joined_responses_and_traces_anon_{0}.csv".format(lang)),
                         index=False)
        print("finished")


def parse_row(line):
    row = line.strip().split('\t')
    if len(row) != 5:
        return None

    d = {'userhash': row[0],
         'geocoded_data': eval(row[1]),
         'has_account': bool(int(row[2])),
         'attempted_edit': bool(int(row[3])),
         'requests': parse_requests(row[4])
         }
    if d['requests'] is None:
        return None
    return d


def parse_requests(requests):
    ret = []
    for r in requests.split(config.request_delim):
        t = r.split('|')
        if (len(t) % 2) != 0:  # should be list of (name, value) pairs and contain at least userhash,ts,title
            continue
        data_dict = {t[i]: t[i + 1] for i in range(0, len(t), 2)}
        ret.append(data_dict)
    try:
        ret.sort(key=lambda x: x['ts'])  # sort by time
    except:
        return None

    return ret


def extract_survey_request(l, lang):
    quicksurvey_dt = datetime.datetime.strptime(str(l.dt_qsinitialization), '%Y-%m-%dT%H:%M:%S')
    timestamp_only = False
    if pd.isnull(l.page_id) and pd.isnull(l.page_title):
        timestamp_only = True
    else:
        page_title = str(l.page_title)
        page_id = int(l.page_id)
    if l.requests is not None:
        for req in reversed(l.requests):
            # same lang as survey was deployed
            if req['lang'] == lang:
                # same page title (no redirects) or same page id / lang (reflects redirects)
                pageview_ts = req["ts"]
                if timestamp_only or ((req["title"] == page_title) or (req["uri_path"] == ("/wiki/" + page_title)) or (
                        int(req['page_id']) == page_id)):
                    if pageview_ts <= quicksurvey_dt:
                        return req
#    print("Not matched: {0}; {1} requests.".format(l.page_title, len(l.requests)))
    return {}


def parse_requests_ts_and_redirects(requests, d={}):
    if len(requests) == 0:
        return None
    for i, r in enumerate(requests):
        r['userhash'] = i
        r['ts'] = datetime.datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')
        if r['title'] in d:
            r['title'] = d[r['title']]
    return requests


if __name__ == "__main__":
    main()
