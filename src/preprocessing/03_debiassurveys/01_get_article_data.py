import argparse
import bz2
from collections import namedtuple
import csv
import gzip
import os
import pickle
import re
import sys

import gensim
from mw.xml_dump import Iterator
import mwparserfromhell
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

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

        # build dictionaries w/ all page IDs in data and all page lengths (not including redirects) in <lang>wiki
        id2title = get_pageids(lang, args)
        id2length = get_id2properties(lang, args.sql_date, args.sql_folder)

        # This just checks if we can find a length for each ID
        id_check(lang, args, id2length, id2title)

        # Build Pandas dataframe with page info
        columns = [(pid, id2title[pid], id2length.get(pid, -1)) for pid in id2title]
        column_names = ["page_id", "page_title", "page_length"]

        # Merge basic page info
        df_pdata = pd.DataFrame(data=columns, columns=column_names)
        print("Length of DF with basic page info:", len(df_pdata))
        df_pdata.set_index('page_id', inplace=True)

        # create the dataframe for the pageviews
        pview_fn = os.path.join(args.pageviews_dir, "{0}_pageviews.csv".format(lang))
        if not os.path.exists(pview_fn):
            get_pageview_data(lang, args.pageviews_dir)
        df_pviews = pd.read_table(pview_fn)
        # page_id, weekly_pageviews
        df_pviews.set_index('page_id', inplace=True)

        # merge page info and page views
        df_pdata_pviews = pd.merge(left=df_pdata, right=df_pviews, left_index=True, right_index=True, how="left")
        print("Length of DF after merging page views:", len(df_pdata_pviews))
        print("Non-null rows:", len(df_pdata_pviews) - df_pdata_pviews.count())
        print("Non-null lengths:", len(df_pdata_pviews[df_pdata_pviews["page_length"] == -1]))

        # Add gensim title for LDA merge
        add_gensim_title(df_pdata_pviews, lang, args.date, args.page_txt_dir)
        print("Pages without gensim title: ", sum(df_pdata_pviews.gensim_title == "-"))
        print("Len DF after merging gensim titles:", len(df_pdata_pviews))

        # Gather LDA features
        df_lda = get_lda_features(args.page_features_dir, lang, args.sql_date)

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


def get_pageids(lang, args):
    """Get dictionary of all page IDs and an associated title (could be redirect)"""
    pageids_fn = os.path.join(args.titles_dir, "titles_{0}.p".format(lang))
    if not os.path.exists(pageids_fn):
        print("Building ID/title dict at {0}".format(pageids_fn))
        df_survey = pd.read_pickle(os.path.join(args.response_dir, "joined_responses_and_traces_{0}.p".format(lang)))
        pageids_survey = get_all_pages(df_survey, lang)
        df_sample = pd.read_pickle(os.path.join(args.sample_dir, "sample_df_{0}.p".format(lang)))
        pageids_sample = get_all_pages(df_sample, lang)
        pids_to_titles = pageids_sample.update(pageids_survey)
        pickle.dump(pids_to_titles, pageids_fn)
    else:
        pids_to_titles = pickle.load(pageids_fn)

    return pids_to_titles

def get_lda_features(page_features_dir, lang, date):
    fn = os.path.join(page_features_dir, "{0}_lda_features.tsv".format(lang))
    if not os.path.exists(fn):
        print("Building LDA features for:", lang)
        ArticleLDA(fn, lang, date).build_topic_model()
    colnames = ['lda_pid'] + ['topic{0}'.format(i) for i in range(config.num_lda_topics)]
    datatypes = {'lda_pid':np.int32}
    datatypes.update({'topic{0}'.format(i):np.float32 for i in range(config.num_lda_topics)})
    df_lda = pd.read_csv(fn, sep='\t',
                         columns=colnames,
                         dtype=datatypes)

    return df_lda

class ArticleLDA:

    def __init__(self, output_features_tsv, lang, date):
        self.output_features_tsv = output_features_tsv
        self.lang = lang
        self.date = date
        self.article_dump = build_local_currentpage_dump_fn(self.lang, self.date)
        self.page_ids = []
        self.page_count = 0
        self.skipped = 0


    def id2text_iterator(self):
        capture_ids = not self.page_ids
        with Iterator.from_file(bz2.BZ2File(self.article_dump, 'r')) as f:
            for page in f:
                if not page.redirect and page.namespace == 0:
                    wikitext = next(page).text
                    plaintext = mwparserfromhell.parse(wikitext).strip_code()
                    self.page_count += 1
                    if capture_ids:
                        self.page_ids.append(page.id)
                    yield plaintext
                else:
                    self.skipped += 1
        if capture_ids:
            print("{0}: {1} pages yielded. {2} skipped.".format(self.article_dump, self.page_count, self.skipped))

    def build_topic_model(self):
        tfidf_model = TfidfVectorizer(max_df=config.lda_max_df,
                                      min_df=config.lda_min_df,
                                      max_features=config.lda_max_features)
        csr_articles = tfidf_model.fit_transform(self.id2text_iterator())
        lda = gensim.models.ldamodel.LdaModel(corpus=gensim.matutils.Sparse2Corpus(csr_articles, documents_columns=False),
                                              id2word={wid:word for word,wid in tfidf_model.vocabulary_.items()},
                                              num_topics=config.num_lda_topics,
                                              update_every=1,
                                              passes=1)

        lda.save(self.output_features_tsv)

def id_check(lang, args, id2props=None, pageids=None):
    if not pageids:
        pass
    if not id2props:
        id2props = get_id2properties(lang, args.sql_date, args.output_dir)

    success = 0
    nonfocal_lang = {}
    missing = {}
    for pid in pageids:
        if pid in id2props:
            success += 1
        else:
            if type(pid) == int:
                missing[pid] = missing.get(pid, 0) + 1
            elif type(pid) == str:
                lang = pid.split(":")[0]
                nonfocal_lang[lang] = nonfocal_lang.get(lang, 0) + 1
            else:
                raise TypeError("Page ID that is not int or str: {0}".format(pid))

    print("Page ID check {0}: missing: {1};  success: {2}".format(lang, len(missing), success))
    print("Non-focal langs+counts: {0}".format(nonfocal_lang))

def get_page_data(lang, output_dir):
    query = "SELECT page_title FROM page WHERE page_namespace = 0"
    filename = os.path.join(output_dir, "{0}_pages.csv".format(lang))
    db = '{0}wiki'.format(lang)
    exec_mariadb_stat2(query, db, filename)

def get_pageview_data(lang, output_dir):
    query = ("SELECT page_id, sum(view_count) AS weekly_pageviews FROM wmf.pageview_hourly "
             "WHERE project = '{0}.wikipedia' "
             "AND agent_type = 'user' "
             "AND {1} "
             "AND namespace_id = 0 "
             "GROUP BY page_id;".format(lang, config.hive_days_clause))
    filename = os.path.join(output_dir, "{0}_pageviews.csv".format(lang))

    exec_hive_stat2(query, filename)

# dictionary of pageid:title (except if non-focal language because page IDs might overlap, then lang-pageid:title)
def get_all_pages(df, lang):
    id_to_title = {}
    for ur in df.requests.apply(lambda x: {int(r['page_id']) if r['lang'] == lang else "{0}:{1}".format(r['lang'], r['page_id']):r["title"] for r in x}):
        id_to_title.update(ur)
    return id_to_title


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

def build_local_currentpage_dump_fn(lang, date):
    local_replicas = '/mnt/data/xmldatadumps/public'
    return os.path.join(local_replicas, '{0}wiki'.format(lang), '{0}wiki-{1}-pages-articles.xml.bz2'.format(lang, date))

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


def get_id2properties(lang, date, output_dir):
    """Build lookup for length of page (bytes)."""
    Page = namedtuple('Page', ['title', 'length'])
    file_path = build_local_currentpage_dump_fn(lang, date)
    id2props = {}
    with Iterator.from_file(bz2.BZ2File(file_path, 'r')) as f:
        for page in f:
            if not page.redirect and page.namespace == 0:
                curr_rev = next(page)
                id2props[page.id] = Page(page.title, len(curr_rev.text))
    return id2props


def add_gensim_title(df, lang, date, output_dir):
    article_text_file = build_article_text_dump_fn(lang, date, output_dir)
    if not os.path.exists(article_text_file):
        download_dumps(lang, date, output_dir, dumptype="article_text")
    langmap = extract_mapping(article_text_file)
    df["gensim_title"] = df["id"].apply(lambda x: langmap[str(x)] if str(x) in langmap else "-")


if __name__ == "__main__":
    main()
