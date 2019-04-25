"""
This script joins:
 * the EventLogging (EL) data based on webrequest beacons (in my experience, most complete / simplest)
 * Google Forms survey responses
 * EditAttemptStep data based on hive tables

There are two outputs for each language:
 * CSV w/ survey responses + EL details (e.g., datetime, pageID) + webrequest details (e.g., client-IP, user-agent)
 * CSV w/ all approximate userhashes for matching against webrequest logs
"""

import argparse
import csv
import os

from geopy.distance import distance
import pandas as pd

# hacky way to make sure utils is visible
import sys
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))

from src.utils import config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--el_logs_fn",
                        default=config.quicksurvey_el_tsv,
                        help="TSV with EventLogging data")
    parser.add_argument("--survey_req_fn",
                        default=config.quicksurvey_requests_tsv,
                        help="TSV with survey webrequests.")
    parser.add_argument("--editattempt_fn",
                        default=config.edit_el_tsv,
                        help="TSV filename for edit attempt data")
    parser.add_argument("--ids_dir",
                        default=config.ids_folder,
                        help="Folder to store survey respondent UserIDs")
    parser.add_argument("--languages",
                        default=config.languages,
                        nargs="*",
                        help="List of languages to process")
    parser.add_argument("--responses_dir",
                        default=config.responses_folder,
                        help="Folder to hold survey responses + associated webrequest")
    parser.add_argument("--dist_threshold",
                        default=config.ip_dist_threshold,
                        help="Max distance in km between Geonames point and IP point for match.")
    parser.add_argument("--geonames_tsv",
                        default=config.geonames_tsv,
                        help="Geonames TSV file w/ place and population information.")

    args = parser.parse_args()

    requests = pd.read_csv(args.survey_req_fn, sep="\t")
    print("{0} total requests.".format(len(requests)))

    requests.drop_duplicates(inplace=True)
    requests.sort_values(by=['response_type'], ascending=False, inplace=True)
    requests.set_index('pageview_token', inplace=True)
    print("{0} requests from {1} unique users after removing duplicates.".format(len(requests),
                                                                                 len(requests['userhash'].unique())))

    map_ip_to_population(requests, args.geonames_tsv, args.dist_threshold)

#    edit_attempts = pd.read_csv(args.editattempt_fn, sep="\t")
#    print("{0} edit actions across {1} users.".format(len(edit_attempts), len(edit_attempts['userhash'].unique())))
#    edit_attempts = edit_attempts.groupby('userhash').apply(group_edit_actions)

    if not os.path.isdir(args.ids_dir):
        print("Creating directory: {0}".format(os.path.abspath(args.ids_dir)))
        os.mkdir(args.ids_dir)

    if not os.path.isdir(args.responses_dir):
        print("Creating directory: {0}".format(os.path.abspath(args.responses_dir)))
        os.mkdir(args.responses_dir)

    all_ids = []
    for lang in args.languages:
        recoded_fn = os.path.join(config.data_folder, "recoded", "responses_{0}_recoded.csv".format(lang))
        surv_responses = pd.read_csv(recoded_fn, sep = '\t')
        surv_responses.set_index('survey_id', inplace=True)
        print("**********")
        print("Google Responses in {0}: {1}".format(lang, len(surv_responses)))

        # merge in quicksurveys eventlogging -- priority to yes to take survey, no to take survey, initiation
        srv_el_req = pd.merge(surv_responses, requests, how="left", left_index=True, right_index=True)
        srv_el_req = srv_el_req[~srv_el_req.index.duplicated(keep='first')]
        print("Breakdown of ability to match up Google responses with EL: (w/o initiation)")
        print(srv_el_req['response_type'].value_counts(dropna=False))
        print("Breakdown of ability to match up Google responses with EL (w/ initiation):")
        print(srv_el_req['country'].value_counts(dropna=False))

        # merge in edit attempt data
#        srv_el_req = srv_el_req.join(edit_attempts, how="left", on="userhash")
#        print("Responses w/ associated edit data (is anon):")
#        print(srv_el_req['is_anon'].value_counts(dropna=False))

        # Write responses+EL+webrequest data to TSV
        output_merged_data = os.path.join(args.responses_dir, "responses_with_el_{0}.csv".format(lang))
        srv_el_req.to_csv(output_merged_data, sep='\t')

        # Write userIDs associated with completed surveys to file
        output_respondent_ids = os.path.join(args.ids_dir, "ids_{0}.csv".format(lang))
        ids = srv_el_req["userhash"]
        ids = ids.dropna()
        ids.to_csv(output_respondent_ids, index=False, header=False)
        print("Complete IDs:", len(ids))

        all_ids.extend(list(ids.values))

    if all_ids:
        with open(config.all_ids_csv, 'w') as fout:
            csvwriter = csv.writer(fout)
            for ip_ua in all_ids:
                csvwriter.writerow([ip_ua])


def group_edit_actions(user_data):
    is_anon = any(user_data['anon'])
    edit_count = user_data['user_edit'].value_counts().index[0]
    editor_interface = user_data['editor_interface'].value_counts().index[0]
    return pd.Series({'is_anon': is_anon,
                      'edit_count': edit_count,
                      'editor_interface':editor_interface})


def map_ip_to_population(df, geonames_tsv, dist_threshold):
    print("Loading geonames lookup")
    geonames = get_geonames_map(geonames_tsv)
    print("Calculating populations")
    df['population'] = df.apply(lambda x: lookup_row(x, geonames, dist_threshold=dist_threshold), axis=1)
    print("Success rate:", (df['population'] > 1).sum() / (df['population'] > 1).count())

def calc_dist(pt1, pt2):
    return distance(pt1, pt2).kilometers

def get_geonames_map(allcountries):
    geonames_header = ['geonameid', 'name', 'asciiname', 'alternatenames',
                       'latitude', 'longitude', 'feature class', 'feature code',
                       'country code', 'cc2', 'admin1 code', 'admin2 code', 'admin3 code', 'admin4 code',
                       'population', 'elevation', 'dem', 'timezone', 'modification date']
    country_idx = geonames_header.index('country code')
    pop_idx = geonames_header.index('population')
    lat_idx = geonames_header.index('latitude')
    lon_idx = geonames_header.index('longitude')
    name_idx = geonames_header.index('name')
    altname_idx = geonames_header.index('alternatenames')
    feature_idx = geonames_header.index('feature class')

    lookup = {}
    num_countries = 0
    num_places = 0
    num_pops = 0
    nonzero_pops = 0
    duplicates = 0
    with open(allcountries, 'r') as fin:
        tsvreader = csv.reader(fin, delimiter='\t')
        for line in tsvreader:
            feature = line[feature_idx]
            try:
                population = int(line[pop_idx])
            except ValueError:
                population = -1
            if (feature == 'A' and population >= 0) or feature == 'P':
                pt = (float(line[lat_idx]), float(line[lon_idx]))
                names = [line[name_idx]]
                if line[altname_idx]:
                    names.extend(line[altname_idx].split(','))
                country = line[country_idx]
                if country not in lookup:
                    num_countries += 1
                    lookup[country] = {}
                for n in names:
                    if n in lookup[country]:
                        if pt in lookup[country][n]:
                            existing_pop = lookup[country][n][pt]
                            if not population:
                                continue
                            elif existing_pop == population:
                                continue
                            elif not existing_pop:
                                lookup[country][n][pt] = population
                                num_pops += 1
                            else:
                                duplicates += 1
                        else:
                            lookup[country][n][pt] = population
                            num_places += 1
                            if num_places % 500000 == 0:
                                print(num_places, "added.")
                            if population >= 0:
                                num_pops += 1
                                if population == 0:
                                    nonzero_pops += 1
                    else:
                        lookup[country][n] = {pt:population}
                        num_places += 1
                        if num_places % 500000 == 0:
                            print(num_places, "added.")
                        if population >= 0:
                            num_pops += 1
                            if population == 0:
                                nonzero_pops += 1
    print("{0} countries. {1} places. {2} places w/ population. {3} w/ pop 0. {4} duplicates".format(
        num_countries, num_places, num_pops, nonzero_pops, duplicates))
    return lookup

def lookup_row(x, geonames, dist_threshold):
    country = x['country_code']
    city = x['city']
    pt = (float(x['lat']), float(x['lon']))
    try:
        candidates = geonames[country][city]
        within_thres = []
        for cpt, cpop in candidates.items():
            if calc_dist(pt, cpt) < dist_threshold:
                within_thres.append(cpop)
        if within_thres:
            return max(within_thres)
        else:
            return -2
    except KeyError:
        return -3


if __name__ == "__main__":
    main()