import argparse
import datetime
import os
import random

import pandas as pd

# hacky way to make sure utils is visible
import sys
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config
from src.utils import read_redirects
from src.utils import main_page_titles

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--languages",
                        default=config.languages,
                        nargs="*",
                        help="List of languages to process")
    parser.add_argument("--in_dir_traces",
                        default=config.smpl_req_folder,
                        help="Folder with webrequest traces")
    parser.add_argument("--redirect_dir",
                        default=config.redirect_folder,
                        help="Folder with Wikipedia redirects")
    parser.add_argument("--out_dir",
                        default=config.smpl_anon_folder,
                        help="Folder for output joined responses/traces.")
    args = parser.parse_args()

    if not os.path.isdir(args.out_dir):
        print("Creating directory: {0}".format(os.path.abspath(args.out_dir)))
        os.mkdir(args.out_dir)

    for lang in args.languages:
        print("**************")
        print("* Processing " + lang)
        print("**************")

        error_count = 0
        success_count = 0
        none_count = 0

        redirects = read_redirects(lang, args.redirect_dir)

        instances = []
        with open(os.path.join(args.in_dir_traces, "parsed_{0}.csv".format(lang)), "w") as out:
            with open(os.path.join(args.in_dir_traces, "sample_{0}.csv".format(lang)), 'r') as f:
                for i, row in enumerate(f):
                    if i > 0:
                        try:
                            row = parse_row(row, redirects, lang)
                            if row is not None:
                                instances.append(row)
                                out.write(str(row) + "\n")
                                success_count += 1
                            else:
                                none_count += 1
                        except Exception as e:
                            print(e)
                            error_count += 1
                        if i % 10000 == 0:
                            print("processing line...", i)
                print("line count: ", i)

        df = pd.DataFrame(instances, columns=["userhash", "geocoded_data", "survey_request", "requests"])
        print("size df: ", len(df))
        df['survey_dt_utc'] = df['survey_request'].apply(lambda x: x['ts'])
        df['survey_title'] = df['survey_request'].apply(lambda x: x['title'])

        df.dropna(inplace=True, subset=['requests'])
        df = df.reset_index(drop=True)
        print("size after dropping users without request: ", len(df))

        print("# errors: ", error_count)
        print("# success: ", success_count)
        print("# nones: ", none_count)

        print("Anonymizing survey...")

        geo_cols = ["continent", "country", "country_code", "timezone"]
        for geo_col in geo_cols:
            df[geo_col] = df['geocoded_data'].apply(lambda x: x.get(geo_col, None))
        df.to_pickle(os.path.join(args.out_dir, "sample_df_{0}.p".format(lang)))
        print("finished")



def parse_row(line, redirects, lang):
    row = line.strip().split('\t')
    if len(row) != 3:
        return None
    
    d = {'userhash': row[0],
         'geocoded_data' : eval(row[1]),
         'requests' : parse_requests(row[2])
        }
    if d['requests'] is None:
        return None

    # select "survey" request
    title_check = ["hyphen-minus"]
    out = []
    for i, r in enumerate(d["requests"]):
        r["userhash"] = i
        if "title" in r and r['lang'] == lang:
            if r['title'] in redirects:
                r['title'] = redirects[r['title']]
            if r['title'] == main_page_titles.get(lang, None):
                continue
            if not any(x in r["title"].lower() for x in title_check):
                r['ts'] = datetime.datetime.strptime(r['ts'], '%Y-%m-%d %H:%M:%S')
                out.append(r)
    if len(out) > 0:
        d["survey_request"] = random.choice(out)
        return d
    else:
        return None



def parse_requests(requests):
    ret = []
    for r in requests.split(config.request_delim):
        t = r.split('|')
        if (len(t) % 2) != 0: # should be list of (name, value) pairs and contain at least userhash,ts,title
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


if __name__ == "__main__":
    main()
