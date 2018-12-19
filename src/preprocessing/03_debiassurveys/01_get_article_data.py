import argparse
import bz2
import csv
import gzip
import os
import pickle
import re
import sys
import xml.etree.ElementTree as et

import pandas as pd

# hacky way to make sure utils is visible
sys.path.append(os.path.abspath(os.path.abspath(os.path.dirname(__file__)) + '/../../..'))
from src.utils import config
from src.utils import download_dump_file
from src.utils import exec_mariadb_stat2
from src.utils import exec_hive_stat2

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--languages",
                        nargs="*",
                        default=config.languages,
                        help="List of languages to process.")
    parser.add_argument("--data_dir",
                        default=config.data_folder,
                        help="Top level folder holding supporting data.")
    parser.add_argument("--redirect_folder",
                        default=config.features_folder,
                        help="Folder for pickled feature DFs.")
    parser.add_argument("--response_dir",
                        default=config.srvy_anon_folder,
                        help="Folder with joined responses / traces.")
    parser.add_argument("--sample_dir",
                        default=config.smpl_anon_folder,
                        help="Folder with control sample traces.")
    parser.add_argument("--titles_dir",
                        default=config.titles_folder,
                        help="Folder for article titles.")
    parser.add_argument("--sql_folder",
                        default=config.page_sql_folder,
                        help="Folder with SQL dumps.")
    parser.add_argument("--sql_date",
                        default=config.sql_date,
                        help="Formatted date of wiki dump")
    parser.add_argument("--pageviews_dir",
                        default=config.pageviews_folder,
                        help="Folder containing pageview data for articles.")
    parser.add_argument("--page_txt_dir",
                        default=config.page_txt_folder,
                        help="Folder containing bz2 dump of article text.")
    parser.add_argument("--page_features_dir",
                        default=config.page_data_folder,
                        help="Folder containing LDA models and other processed page data.")
    parser.add_argument("--article_graph_dir",
                        default=config.article_graph_folder,
                        help="Folder containing graph features for articles.")
    parser.add_argument("--article_dir",
                        default=config.article_folder,
                        help="Folder with article-specific features")
    args = parser.parse_args()

    for lang in args.languages:
        print("\n\n\n====== {0} ======".format(lang))
        # build lists w/ all titles and redirects for each language
        titles = get_title_list(lang, args)
        redirected = get_redirects(lang, args, titles)

        # This just checks if we can find a title
        title2id = get_title2id(lang, args.date, args.sql_folder)
        title_check(lang, args, redirected, title2id)

        # Get page lengths + IDs
        ids = [title2id[t] if t in title2id else -1 for t in redirected]
        page_lengths = get_id2length(lang, args.date, args.sql_folder)
        page_lengths_column = [page_lengths[i] if i in page_lengths else 0 for i in ids]

        # Merge basic page info
        df_pdata = pd.DataFrame(
            data={"title": titles, "redirected_title": redirected, "id": ids, "page_lengths": page_lengths_column})
        print("Length of DF with basic page info:", len(df_pdata))
        print("Pages without ID:", sum(df_pdata["id"] == -1))

        # create the dataframe for the pageviews
        pview_fn = os.path.join(args.pageviews_dir, f"{lang}_pageviews.csv")
        if not os.path.exists(pview_fn):
            get_pageview_data(lang, args.pageviews_dir)
        df_pviews = pd.read_table(pview_fn)
        df_pviews["page_title"] = df_pviews["page_title"].apply(lambda x: str(x).lower())
        df_pviews = df_pviews.groupby("page_title").max()
        df_pviews["page_title"] = df_pviews.index

        # merge page info and page views
        df_pdata_pviews = pd.merge(left=df_pdata, right=df_pviews, left_on="redirected_title", right_on="page_title",
                           how="left")
        print("Length of DF after merging page views:", len(df_pdata_pviews))
        print("Non-null rows:", len(df_pdata_pviews) - df_pdata_pviews.count())
        print("Non-null IDs:", len(df_pdata_pviews[df_pdata_pviews["id"] == -1]))

        # Add gensim title for LDA merge
        add_gensim_title(df_pdata_pviews, lang, args.date, args.page_txt_dir)
        print("Pages without gensim title: ", sum(df_pdata_pviews.gensim_title == "-"))
        print("Len DF after merging gensim titles:", len(df_pdata_pviews))

        # Gather LDA features
        data = []
        with open(os.path.join(args.page_features_dir, f"{lang}_lda_features.csv"), "r") as fin:
            tsvreader = csv.reader(fin, delimiter="\t")
            for tokens in tsvreader:
                data.append(tokens)
        columns = ["gensim_title"] + [f"topic_{x}" for x in range(config.num_lda_topics)]
        df_lda = pd.DataFrame(data, columns=columns)

        # convert feature columns to numeric values
        for i in range(config.num_lda_topics):
            df_lda[f"topic_{i}"] = pd.to_numeric(df_lda[f"topic_{i}"])

        # merge the lda features
        df_with_topics = pd.merge(left=df_pdata_pviews, right=df_lda, how="left", on="gensim_title")
        # Compute "main topic"
        df_with_topics["main_topic"] = df_with_topics[columns[1:]].idxmax(axis=1)
        print("# articles with topics:", df_with_topics.count())


        # Join Network features
        df_graph = pd.read_csv(os.path.join(args.article_graph_dir, f"{lang}_graph_features.csv"), header=None,
                               names=["id", "pagerank", "indegree", "outdegree"])
        df_all = pd.merge(left=df_with_topics, right=df_graph, on="id", how="left")
        pickle.dump(df_all, open(os.path.join(args.article_dir, f"article_features_{lang}.p", "wb")))

        for c in df_all.columns:
            print(df_all[c].describe())


def get_title_list(lang, args):
    titles_fn = os.path.join(args.titles_dir, f"titles_{lang}.p")
    if not os.path.exists(titles_fn):
        print("Building title list at {0}".format(titles_fn))
        df_survey = pd.read_pickle(os.path.join(args.response_dir, f"joined_responses_and_traces_{lang}.p"))
        titles_survey = get_all_titles(df_survey)
        df_sample = pd.read_pickle(os.path.join(args.sample_dir, f"sample_df_{lang}.p"))
        titles_sample = get_all_titles(df_sample)
        all_titles = titles_sample.union(titles_survey)
        all_titles = list(all_titles)
        pickle.dump(all_titles, titles_fn)
    else:
        all_titles = pickle.load(titles_fn)

    return all_titles

def get_redirects(lang, args, all_titles=None):
    redirects_fn = os.path.join(args.titles_dir, f"redirected_{lang}.p")
    if not all_titles:
        all_titles = get_title_list(lang, args)

    if not os.path.exists(redirects_fn):
        redirects = load_redirects(lang, args.redirect_folder)
        redirected = [redirects[t] if t in redirects else t for t in all_titles]
        pickle.dump(redirected, redirects_fn)
        print("# All Titles:", len(all_titles))
    else:
        redirected = pickle.load(redirects_fn)

    return redirected

def title_check(lang, args, redirects=None, title2id=None):
    if not title2id:
        title2id = get_title2id(lang, args.date, args.sql_folder)
    if not redirects:
        redirects = get_redirects(lang, args)

    success = 0
    error = 0
    for t in redirects:
        if t in title2id:
            success += 1
        else:
            error += 1

    print(f"Title check {lang}: error: {error}; success: {success}")

def get_page_data(lang, output_dir):
    query = "SELECT page_title FROM page WHERE page_namespace = 0"
    filename = os.path.join(output_dir, "{0}_pages.csv".format(lang))
    db = '{0}wiki'.format(lang)
    exec_mariadb_stat2(query, db, filename)

def get_pageview_data(lang, output_dir):
    query = ("SELECT page_title, sum(view_count) AS weekly_pageviews FROM wmf.pageview_hourly "
             "WHERE project = '{0}.wikipedia' "
             "AND agent_type = 'user' "
             "AND {1} "
             "AND namespace_id = 0 "
             "GROUP BY page_title;".format(lang, config.hive_days_clause))
    filename = os.path.join(output_dir, "{0}_pageviews.csv".format(lang))

    exec_hive_stat2(query, filename)

# Select page titles
def get_all_titles(df):
    all_titles_survey = []
    for x in df.requests.apply(lambda x: [r["title"].lower() for r in x]):
        all_titles_survey.extend(x)
    return set(all_titles_survey)


def load_redirects(lang, redirect_folder):
    redirects = {}
    with open(os.path.join(redirect_folder, "{0}_redirect.tsv".format(lang)), "r") as fin:
        tsvreader = csv.reader(fin, delimiter="\t")
        for line in tsvreader:
            if len(line) != 2:
                print("error parsing line", line)
                continue
            redirects[line[0].lower()] = line[1].lower()
    return redirects

def build_article_text_dump_fn(lang, date, output_dir):
    return os.path.join(output_dir, "[0}wiki-{1}-pages-articles.xml.bz2".format(lang, date))

def build_sql_dump_fn(lang, date, output_dir):
    return os.path.join(output_dir, "{0}wiki-{1}-page.sql.gz".format(lang, date))

def download_dumps(lang, date, output_dir, dumptype="sql"):
    """WGET a dump file to local machine"""
    base_url = "https://dumps.wikimedia.org/{0}wiki/{1}"
    if dumptype == "sql":
        dump_url = build_sql_dump_fn(lang ,date, base_url)
        output_fn = build_sql_dump_fn(lang, date, output_dir)
    elif dumptype == "article_text":
        dump_url = build_article_text_dump_fn(lang, date, base_url)
        output_fn = build_article_text_dump_fn(lang, date, output_dir)
    else:
        raise ValueError("Dumptype must be sql or article_text: {0}".format(dumptype))
    download_dump_file(dump_url, output_fn)

def get_title2id(lang, date, output_dir):
    """Build lookup for title -> page ID."""
    file_path = build_sql_dump_fn(lang, date, output_dir)
    if not os.path.exists(file_path):
        download_dumps(lang, date, output_dir, dumptype="sql")
    title2id = {}
    with gzip.open(file_path, "r") as f:
        for line in f:
            line = line.decode("utf-8")
            if line.startswith("INSERT INTO"):
                pages = line.split("),(")
                pages[0] = pages[0].replace("INSERT INTO `page` VALUES (", "")
                pages[-1] = pages[-1].replace(";\n", "")
                for p in pages:
                    tokens = re.findall(r"(?:[^\s,']|'(?:\\.|[^'])*')+", p)
                    page_id = int(tokens[0])
                    page_namespace = tokens[1]
                    page_title = tokens[2].lower()[1:-1]
                    is_redirect = bool(int(tokens[5]))
                    if page_namespace == "0" and not is_redirect:
                        title2id[page_title] = page_id
    return title2id


def get_id2length(lang, date, output_dir):
    """Build lookup for length of page (bytes)."""
    file_path = build_sql_dump_fn(lang, date, output_dir)
    if not os.path.exists(file_path):
        download_dumps(lang, date, output_dir, dumptype="sql")
    result = {}
    with gzip.open(file_path, "r") as f:
        for line in f:
            line = line.decode("utf-8")
            if line.startswith("INSERT INTO"):
                pages = line.split("),(")
                pages[0] = pages[0].replace("INSERT INTO `page` VALUES (", "")
                pages[-1] = pages[-1].replace(";\n", "")

                for p in pages:
                    tokens = re.findall(r"(?:[^\s,']|'(?:\\.|[^'])*')+", p)
                    page_id = int(tokens[0])
                    page_namespace = tokens[1]
                    page_length = int(tokens[11])
                    is_redirect = bool(int(tokens[5]))
                    if page_namespace == "0" and not is_redirect:
                        result[page_id] = page_length
    return result


# extract map from "gensim"-title to page_id
def extract_mapping(filename):
    with bz2.BZ2File(filename, "r") as f:
        mapping = {}
        current_page = ""
        page_count = 0
        for i, line in enumerate(f):
            line = line.strip().decode("utf-8")
            if line.startswith("<page>"):
                current_page = ""
                page_count += 1
            current_page += line
            if line.startswith("</page>"):
                page_elem = et.fromstring(current_page)
                title = page_elem.find("title").text
                pid = page_elem.find("id").text
                mapping[pid] = title
                if page_count % 100000 == 0:
                    print("extracted page titles:", page_count)

    return mapping


def add_gensim_title(df, lang, date, output_dir):
    article_text_file = build_article_text_dump_fn(lang, date, output_dir)
    if not os.path.exists(article_text_file):
        download_dumps(lang, date, output_dir, dumptype="article_text")
    langmap = extract_mapping(article_text_file)
    df["gensim_title"] = df["id"].apply(lambda x: langmap[str(x)] if str(x) in langmap else "-")


if __name__ == "__main__":
    main()
