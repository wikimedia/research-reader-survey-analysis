"""
This script joins:
 * the EventLogging (EL) data from MariaDB
 * Google Forms survey responses
 * Hive webrequests that contain QuickSurvey beacons

There are two outputs for each language:
 * CSV w/ survey responses + EL details (e.g., datetime, pageID) + webrequest details (e.g., client-IP, user-agent)
 * CSV w/ all approximate userIDs for matching against webrequest logs
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


    # EventLogging entries for our surveys (all languages)
    el_logs = pd.read_csv(args.el_logs_fn, sep="\t")

    # All surveySessionTokens recorded by EventLogging
    session_tokens = frozenset(el_logs["event_surveySessionToken"])

    # Load in webrequests data and filter by matching to EL
    relevant_lines = []
    header = ['dt', 'id', 'survey_session_token']
    sst_idx = header.index('survey_session_token')
    exp_num_fields = len(header)
    with open(args.survey_req_fn) as f:
        csvreader = csv.reader(f, delimiter="\t")
        for i, tokens in enumerate(csvreader):
            if tokens[sst_idx] in session_tokens and len(tokens) == exp_num_fields:
                relevant_lines.append(tokens)
            if i % 10000000 == 0:
                num_relevant = len(relevant_lines)
                print("line {0} => {1} ({2:.1f}%)".format(i, num_relevant, num_relevant * 100 / i))

    requests = pd.DataFrame(relevant_lines, columns=header)
    print("{0} total requests.".format(len(requests)))

    requests.drop_duplicates(inplace=True)
    print("{0} requests after removing duplicates.".format(len(requests)))

    if not os.path.isdir(args.ids_dir):
        print("Creating directory: {0}".format(os.path.abspath(args.ids_dir)))
        os.mkdir(args.ids_dir)

    if not os.path.isdir(args.responses_dir):
        print("Creating directory: {0}".format(os.path.abspath(args.responses_dir)))
        os.mkdir(args.responses_dir)

    all_ids = []
    for lang in args.languages:
        recoded_fn = os.path.join(config.data_folder, "recoded", "responses_{0}_recoded.csv".format(lang))
        surv_responses = pd.read_csv(recoded_fn, sep = ',')
        surv_responses["response_id"] = surv_responses.index
        print("**********")
        print("Google Responses in {0}: {1}".format(lang, len(surv_responses)))

        # Merge the survey responses with the EL data first.
        # We want specifically the survey_session information
        # NOTE: inner join will take the first matching record from EventLogging
        srv_el = pd.merge(surv_responses, el_logs, how="inner",
                          left_on="survey_id", right_on="event_surveyInstanceToken", left_index=True)
        srv_el.drop_duplicates(subset=["response_id"], inplace=True)
        print("Google Responses with an EL: {0}".format(len(srv_el) - srv_el['event_surveySessionToken'].isnull().sum()))

        # Merge the survey+EL data with the Hive webrequests
        srv_el_req = pd.merge(srv_el, requests, how="inner",
                              left_on="event_surveySessionToken", right_on="survey_session_token", left_index=True)
        srv_el_req.drop_duplicates(subset=["response_id"], inplace=True)
        print("Google Responses with an IP: {0}".format(len(srv_el_req) - srv_el_req['client_ip'].isnull().sum()))

        # Write responses+EL+webrequest data to TSV
        output_merged_data = os.path.join(args.responses_dir, "responses_with_ip_{0}.csv".format(lang))
        srv_el_req.to_csv(output_merged_data, sep='\t')

        # Write userIDs associated with completed surveys to file
        output_respondent_ids = os.path.join(args.ids_dir, "ids_{0}.csv".format(lang))
        ids = srv_el_req["id"]
        ids.dropna(subset=["id"], inplace=True)
        ids.to_csv(output_respondent_ids, quoting=csv.QUOTE_ALL, index=False, header=False)
        print("Complete IDs:", len(ids))

        all_ids.extend(list(srv_el_req.values))

    if all_ids:
        with open(config.all_ids_csv) as fout:
            csvwriter = csv.writer(fout)
            for ip_ua in all_ids:
                csvwriter.writerow([ip_ua])


if __name__ == "__main__":
    main()