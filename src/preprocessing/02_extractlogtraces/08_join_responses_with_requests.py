import argparse
import datetime
import os
import pickle

import pandas as pd

# hacky way to make sure utils is visible
import sys
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config

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

    columns_to_keep = ["geocoded_data",
                       "requests",
                       "submit_timestamp",
                       "raw_information_depth",
                       "raw_prior_knowledge",
                       "raw_motivation",
                       "information depth",
                       "prior knowledge",
                       "motivation",
                       "timestamp",
                       "webHost",
                       "wiki",
                       "dt",
                       "survey_request",
                       "survey_dt_utc"]


    for lang in args.languages:
        print("**************")
        print("* Processing " + lang)
        print("**************")
        with open(os.path.join(args.in_dir_traces, "sample_{0}.csv".format(lang)), "r") as f:
            l_count = 0
            lines = []
            for line in f:
                if l_count > 0:
                    l = parse_row(line)
                    if l is not None:
                        lines.append(l)
                if l_count % 10000 == 9999:
                    print("processing line...", l_count + 1)
                l_count = l_count + 1
            print("line count: ", l_count)

        df_traces = pd.DataFrame(lines)
        print("len of df", len(lines))
        print("len of df", len(df_traces))

        df_responses = pd.read_csv(os.path.join(args.in_dir_responses, "responses_with_ip_{0}.csv".format(lang)), sep="\t")
        print("responses with duplicates: ", len(df_responses))
        df_responses.drop_duplicates(subset=["id"], inplace=True)
        print("responses without duplicates: ", len(df_responses))

        print("traces with duplicates", len(df_traces))
        df_traces.drop_duplicates(subset=["id", "geo_data"], inplace=True)
        print("traces without duplicates", len(df_traces))

        df_merged = pd.merge(df_traces, df_responses, left_on=["id"],
                             right_on=["id"], how="inner")
        print("in merged dataframe: ", len(df_merged))

        df_merged['requests'] = df_merged['requests'].apply(parse_requests_ts_and_redirects, d=read_redirects(lang, args.redirect_dir))
        df_merged['survey_request'] = df_merged.apply(extract_survey_request, axis=1)
        df_merged.dropna(subset=["survey_request"], inplace=True)
        df_merged['survey_dt_utc'] = df_merged['survey_request'].apply(lambda x: x['ts'])
        print("After removing non-existing survey requests: ", len(df_merged))
        df_merged = df_merged.reset_index(drop=True)

        print("Anonymizing survey...")
        df_merged = df_merged[columns_to_keep]
        df_merged['geocoded_data'] = df_merged['geocoded_data'].apply(
            lambda x: {k: x[k] for k in ["continent", "country", "country_code", "timezone"]})
        pickle.dump(df_merged, open(os.path.join(args.out_dir, "joined_responses_and_traces_anon_{0}.p".format(lang)), "wb"))
        df_merged.to_csv(os.path.join(args.out_dir, "joined_responses_and_traces_anon_{0}.csv".format(lang)), index=False)
        print("finished")


def read_redirects(lang, redirect_dir):
    redirect_dict = {}
    with open(os.path.join(redirect_dir, "{0}_redirect.tsv".format(lang)), "r") as f:
        for line in f:
            tokens = line.split("\t")
            source = tokens[0].strip()
            if source.startswith('"') and source.endswith('"'):
                source = source[1:-1]
            target = tokens[1].strip()
            if target.startswith('"') and target.endswith('"'):
                target = target[1:-1]
            redirect_dict[source] = target
    return redirect_dict

def parse_row(line):
    row = line.strip().split('\t')
    if len(row) != 3:
        return None
    
    d = {'id': row[0],
         'geocoded_data' : eval(row[1]),
         'requests' : parse_requests(row[2])
        }
    if d['requests'] is None:
        return None
    return d


def parse_requests(requests):
    ret = []
    for r in requests.split('REQUEST_DELIM'):
        t = r.split('|')
        if (len(t) % 2) != 0: # should be list of (name, value) pairs and contain at least id,ts,title
            continue
        data_dict = {t[i]:t[i+1] for i in range(0, len(t), 2)}
        ret.append(data_dict)
    try:
        ret.sort(key = lambda x: x['ts']) # sort by time
    except:
        return None
    
    return ret


def extract_survey_request (l):
    pageTitle = str(l.event_pageTitle)
    dt = datetime.datetime.strptime(str(l.timestamp), '%Y%m%d%H%M%S')
    if l.requests is None:
        return None
    for req in reversed(l.requests):
        if (req["title"] == pageTitle) or (req["uri_path"] == ("/wiki/" + pageTitle)):
            ts = req["ts"]
            if ts < dt:
                return req
    return None

def parse_requests_ts_and_redirects(requests, d={}):
    if len(requests) == 0:
        return None
    for i, r in enumerate(requests):
        r['id'] = i
        r['ts'] = datetime.datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')
        if r['title'] in d:
            r['title'] = d[r['title']]
    return requests

if __name__ == "__main__":
    main()
