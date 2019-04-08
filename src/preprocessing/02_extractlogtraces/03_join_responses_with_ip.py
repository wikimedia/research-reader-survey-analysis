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

    args = parser.parse_args()

    requests = pd.read_csv(args.survey_req_fn, sep="\t")
    print("{0} total requests.".format(len(requests)))

    requests.drop_duplicates(inplace=True)
    requests.sort_values(by=['response_type'], ascending=False, inplace=True)
    requests.set_index('pageview_token', inplace=True)
    print("{0} requests after removing duplicates.".format(len(requests)))

    edit_attempts = pd.read_csv(args.editattempt_fn, sep="\t")
    print("{0} edit actions across {1} users.".format(len(edit_attempts), len(edit_attempts['userhash'].unique())))
    edit_attempts = edit_attempts.groupby('userhash').apply(group_edit_actions)

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
        srv_el_req = srv_el_req.join(edit_attempts, how="left", on="userhash")
        print("Responses w/ associated edit data (is anon):")
        print(srv_el_req['is_anon'].value_counts(dropna=False))

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


if __name__ == "__main__":
    main()