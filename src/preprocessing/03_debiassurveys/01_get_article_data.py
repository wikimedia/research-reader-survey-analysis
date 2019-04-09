import argparse
import bz2
from collections import namedtuple
import csv
import os
import pickle
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

        # Gather LDA features
        df_lda = get_lda_features(args.page_features_dir, lang, args.sql_date)

        # merge the lda features
        df_with_topics = pd.merge(left=df_pdata_pviews, right=df_lda, how="left", left_index=True, right_index=True)
        print("# articles with topics:", df_with_topics['main_topic'].count())

        # Join Network features
        df_graph = pd.read_csv(os.path.join(args.article_graph_dir, "{0}_graph_features.csv".format(lang)), header=None,
                               names=["id", "pagerank", "indegree", "outdegree"])
        df_graph.set_index('id', inplace=True)
        df_all = pd.merge(left=df_with_topics, right=df_graph, how="left", left_index=True, right_index=True)
        with open(os.path.join(args.article_dir, "article_features_{0}.p".format(lang)), 'wb') as fout:
            pickle.dump(df_all, fout)

        for c in df_all.columns:
            print(df_all[c].describe())


def get_pageids(lang, args):
    """Get dictionary of all page IDs and an associated title (could be redirect)"""
    pageids_fn = os.path.join(args.titles_dir, "titles_{0}.p".format(lang))
    if not os.path.exists(pageids_fn):
        print("Building ID/title dict at {0}".format(pageids_fn))
        df_survey = pd.read_pickle(os.path.join(args.response_dir, "joined_responses_and_traces_anon_{0}.p".format(lang)))
        pageids_survey = get_all_pages(df_survey, lang)
        df_sample = pd.read_pickle(os.path.join(args.sample_dir, "sample_df_{0}.p".format(lang)))
        pageids_sample = get_all_pages(df_sample, lang)
        pids_to_titles = pageids_sample.update(pageids_survey)
        with open(pageids_fn, 'wb') as fout:
            pickle.dump(pids_to_titles, fout)
    else:
        with open(pageids_fn, 'rb') as fin:
            pids_to_titles = pickle.load(fin)

    return pids_to_titles

def get_lda_features(page_features_dir, lang, date):
    fn = os.path.join(page_features_dir, "{0}_lda_features.tsv".format(lang))
    if not os.path.exists(fn):
        print("Building LDA features for:", lang)
        ArticleLDA(page_features_dir, lang, date).build_topic_model()
    colnames = ['lda_pid'] + ['topic{0}'.format(i) for i in range(config.num_lda_topics)]
    datatypes = {'lda_pid':np.int32}
    datatypes.update({'topic{0}'.format(i):np.float32 for i in range(config.num_lda_topics)})
    df_lda = pd.read_csv(fn, sep='\t',
                         columns=colnames,
                         dtype=datatypes)
    df_lda.set_index('lda_pid', inplace=True)

    return df_lda

class ArticleLDA:

    def __init__(self, output_features_dir, lang, date, id2title=None):
        self.output_features_tsv = os.path.join(output_features_dir, "{0}_lda_features.tsv".format(lang))
        self.output_lda = os.path.join(output_features_dir, '{0}.lda'.format(lang))
        self.output_overview = os.path.join(output_features_dir, '{0}_overview.tsv'.format(lang))
        self.lang = lang
        self.date = date
        self.article_dump = build_local_currentpage_dump_fn(self.lang, self.date)
        self.page_ids = []
        self.page_count = 0
        self.skipped = 0
        self.id2title = id2title


    def id2text_iterator(self):
        capture_ids = not self.page_ids
        with bz2.BZ2File(self.article_dump, 'r') as fin:
            d = Iterator.from_file(fin)
            for page in d:
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
        corpus = tfidf_model.fit_transform(self.id2text_iterator())
        corpus = gensim.matutils.Sparse2Corpus(corpus, documents_columns=False)
        id2word = {wid:word for word,wid in tfidf_model.vocabulary_.items()}
        lda = gensim.models.ldamodel.LdaModel(corpus=corpus,
                                              id2word=id2word,
                                              num_topics=config.num_lda_topics,
                                              update_every=1,
                                              passes=1)

        # save LDA model features
        lda.save(self.output_lda)

        # save topic distribution for each article
        page_reprs = []
        for i, page_repr in enumerate(lda[corpus]):
            page_repr_alldim = [0.0] * config.num_lda_topics
            for topic_idx, topic_prop in page_repr:
                page_repr_alldim[topic_idx] = topic_prop
            page_reprs.append([self.page_ids[i], page_repr[0][0]] + page_repr_alldim)

        page_reprs = pd.DataFrame(page_reprs,
                                  columns=['lda_pid', 'main_topic'] + ['topic{0}'.format(i) for i in range(config.num_lda_topics)])
        page_reprs.set_index('lda_pid', inplace=True)
        page_reprs.to_csv(self.output_features_tsv, sep="\t")

        # save LDA overview
        topic_overview = pd.DataFrame(columns=['topic', 'top words', 'top articles', 'prevalence_top1', 'prevalence_top3'])
        prev_top = page_reprs.apply(lambda x: np.argsort(x)[::-1][:3], axis=1)
        prev_top1 = prev_top.iloc[:,0].value_counts()
        prev_top3 = prev_top1.add(prev_top.iloc[:,1].value_counts(), fill_value=0).add(prev_top.iloc[:,2].value_counts(), fill_value=0)
        for topic_idx in range(config.num_lda_topics):
            top_words = [w for w,prop in lda.show_topic(topic_idx, 20)]
            top_articles = list(page_reprs['topic{0}'.format(topic_idx)].sort_values()[-20:].index.values)
            if self.id2title:
                top_articles = [self.id2title.get(pid) for pid in top_articles]
            topic_overview.append({'topic':topic_idx,
                                   'top words':top_words,
                                   'top articles':top_articles,
                                   'prevalence_top1':prev_top1.get(topic_idx, 0),
                                   'prevalence_top3':prev_top3.get(topic_idx, 0)},
                                  ignore_index=True)
        topic_overview.to_csv(self.output_overview, sep='\t')

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


def build_article_text_dump_fn(lang, date, output_dir):
    return os.path.join(output_dir, "[0}wiki-{1}-pages-articles.xml.bz2".format(lang, date))

def build_sql_dump_fn(lang, date, output_dir):
    return os.path.join(output_dir, "{0}wiki-{1}-page.sql.gz".format(lang, date))

def build_local_currentpage_dump_fn(lang, date):
    local_replicas = '/mnt/data/xmldatadumps/public'
    return os.path.join(local_replicas, '{0}wiki'.format(lang), date, '{0}wiki-{1}-pages-articles.xml.bz2'.format(lang, date))

def download_dumps(lang, date, output_dir, dumptype="sql"):
    """WGET a dump file to local machine"""
    base_url = "https://dumps.wikimedia.org/{0}wiki/{1}".format(lang, date)
    if dumptype == "sql":
        dump_url = build_sql_dump_fn(lang ,date, base_url)
        output_fn = build_sql_dump_fn(lang, date, output_dir)
    elif dumptype == "article_text":
        dump_url = build_article_text_dump_fn(lang, date, base_url)
        output_fn = build_article_text_dump_fn(lang, date, output_dir)
    else:
        raise ValueError("Dumptype must be sql or article_text: {0}".format(dumptype))
    download_dump_file(dump_url, output_fn)


def get_id2properties(lang, date, output_dir):
    """Build lookup for length of page (bytes)."""
    Page = namedtuple('Page', ['title', 'length'])
    output_fn = os.path.join(output_dir, '{0}_page_props.tsv'.format(lang))
    id2props = {}
    if os.path.exists(output_fn):
        with open(output_fn, 'r') as fin:
            tsvreader = csv.reader(fin, delimiter="\t")
            for line in tsvreader:
                pid = int(line[0])
                title = line[1]
                plen = int(line[2])
                id2props[pid] = Page(title, plen)
    else:
        file_path = build_local_currentpage_dump_fn(lang, date)
        print("Gathering page properties from dump.")
        with bz2.BZ2File(file_path, 'r') as fin:
            d = Iterator.from_file(fin)
            for i, page in enumerate(d, start=1):
                if not page.redirect and page.namespace == 0:
                    curr_rev = next(page)
                    id2props[page.id] = Page(page.title, len(curr_rev.text))
                if i % 1000000 == 0:
                    print("{0} pages evaluated. {1} retained.".format(i, len(id2props)))
        with open(output_fn, 'w') as fout:
            tsvwriter = csv.writer(fout, delimiter="\t")
            for pid in id2props:
                tsvwriter.writerow([pid, id2props[pid].title, id2props[pid].length])

    return id2props

if __name__ == "__main__":
    main()
