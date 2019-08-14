#!/usr/bin/env python3
# -*- coding=utf8 -*-
"""
Reads a file and executes each line as a query to an
elasticsearch index.
"""

from __future__ import print_function
from operator import itemgetter
from timeit import default_timer
from collections import defaultdict
import numpy as np
import argparse
import re
import sys
import pickle


# local
try:
    # compiled as a package
    import elastify.rank_metrics as rm
    import elastify.utils as utils
except ImportError:
    # started as plain script
    import rank_metrics as rm
    import utils


def join_scores(y_true, y, padding=0, sort=True):
    """join_scores
    :description:
    Take 2 arrays of (key,score) pairs,
    Preserves ranking of y but replaces scores with the ones of y_true
    Useful for evaluating rank metrics ;)
    :param y_true:
    dictionary of relevance scores
    :param y:
    relevance values to evaluate
    :param padding:
    which length should be asserted by padding with zeros
    :param sort:
    if true, sort the relevance values y
    """
    # _y = sorted(y, key=itemgetter(1), reverse=True) if sort else y

    # DCG _y_true and use them as dcg max
    # dcg_at_k(_y_true,k)
    r = [y_true[key] for key in y]

    # padding with zeros up
    r += [0] * (padding - len(r))

    return r


def score(gold, docs, metric, k):
    """score
    Evaluates a metric on Y with Y_true as gold standard.
    :param docs_true:
    Dict of id, relevance pairs to use as gold-standard
    :param docs:
    dict of id, relevance pairs to evaluate
    :param metric:
    metric to use
    :param k:
    k-value for metric
    :param binary:
    binary or exact relevance
    """
    if not len(gold):
        return 1.0

    # TODO move this unzipping somewhere else..
    # y = [(doc['_id'], doc['_score']) for doc in docs]
    # elastics scores are already sorted.
    r = join_scores(gold, docs, padding=k, sort=False)

    if metric == 'precision':
        return rm.precision_at_k(r, k)
    elif metric == 'ndcgg':
        r_gold = sorted(gold.values(), reverse=True)
        dcg_max = rm.dcg_at_k(r_gold, k)
        if not dcg_max:
            return 0
        return rm.dcg_at_k(r, k) / dcg_max
    elif metric == 'ndcg':
        return rm.ndcg_at_k(r, k)
    elif metric == 'map':
        return rm.mean_average_precision([r[:i] for i in range(1, k+1)])
    else:
        raise ValueError


def perform_comparison(goldstandard, challenger, k, metrics,
                       source=True, verbose=0):
    """perform_comparison
    :param goldstandard:
    the gold standard retrieved documents TODO: change this to dict
    :param challenger:
    the challenging strategy's retrieved documents
    :param ks:
    the @k parameter for the metric
    :param metric:
    the metric to use
    :param queries:
    the iterable of queries to use
    :param binary:
    Perform the comparison with binary relevance values.
    :param source: if True, the actual values for the queries are
    retrieved aswell.
    :param verbose, if True prints a status indication to stderr
    """
    results = defaultdict(list)
    assert len(goldstandard) == len(challenger)  # be fair
    for i, (gold, docs) in enumerate(zip(goldstandard, challenger)):
        # if queries:
        #     results.append((queries[i], score(gold, docs[:k], metric, k)))
        # else:
        for metric in metrics:
            results[metric].append(score(gold, docs[:k], metric, k))

        if verbose:
            print("\r[querify] %7d queries evaluated" % i, flush=True, end='',
                  file=sys.stderr)
    if verbose:  # clean up flushing
        print(file=sys.stderr)
    return results


def print_query_results(results, fname=None):
    """print_query_results

    :param results:
    :param fname:
    """
    fhandle = open(fname, 'w') if fname else sys.stdout
    _results = sorted(results, key=itemgetter(1), reverse=True)
    print("| score | query |", file=fhandle)
    print("|-------|-------|", file=fhandle)
    for query, score in _results:
        print("|", score, "|", query, "|", file=fhandle)
    fhandle.close()

def load_documents(docs_path):
    documents = dict()
    docs_list = os.listdir(docs_path)
    for doc in docs_list:
        doc_id = doc.split('.')[0]
        documents[doc_id] = docs_path + '/' + doc

    return documents

def reduce_dicts(titles):
    """ reduce dictionary to list,
    providing 'same index' iff 'same key in the dictionary'
    """
    titles = dict(titles)
    titles_list = []
    for key in titles:
        titles_list.append(key)

    titles_list.sort()
    return titles_list

def _create_parser():
    """Creates an appropriate command line parser

    :returns: ArgumentParser

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('querysource', type=str,
                        help='File of one query string per line OR a single\
                        query to retrieve K results for.')
    parser.add_argument('-i', '--index', default='economics', dest='index',
                        help='Specify the elasticsearch index [economics]')
    parser.add_argument('-d', '--doc-type', dest='doc_type',
                        default='publication',
                        help="Specify the elasticsearch document type,\
                        [publication]")
    parser.add_argument('-g', '--goldstandard', type=str,
                        default='fulltext', help='specify the goldstandard file for the evaluation')
    parser.add_argument('-D', '--documents', type=str,
                        help='specify the goldstandard file for the evaluation')
    parser.add_argument('-T', '--topics', type=str,
                        help='specify the goldstandard file for the evaluation')
    parser.add_argument('-B', '--batches', type=int, nargs='+',
                        default=[5, 5, 5, 5], help="Specify the count of relevant\
                        documents in the gold standard, default=[20,20,20]")
    parser.add_argument('-s', '--strategy',
                        type=str,
                        default=['ctfidf'],
                        nargs='+',
                        choices=list(utils.FIELDS.keys()) + ["_all"],
                        help='specify strategy to use (on titles) [ctfidf]')
    parser.add_argument('-o', '--output',
                        default=sys.stdout, type=argparse.FileType('a'),
                        help='The file to write output to [sys.stdout]')
    parser.add_argument('-k', '--at', type=int, metavar="k",
                        default=20, help='Specify limit for\
                        metric (e.g. ncdg at k) [20]')
    parser.add_argument('-m', '--metric', type=str, nargs='+',
                        default=['precision', 'map', 'ndcg',  'ndcgg'],
                        choices=['ndcg', 'precision', 'map', 'ndcgg'],
                        help='Specify evaluation metric [default: all]')
    parser.add_argument('-t', '--timestamp', action='store_true',
                        default=False,
                        help="Drop the timestamp in queries")
    parser.add_argument('-v', '--verbose', action='count', default=0)
    parser.add_argument('-V', '--very-verbose', action='store_true',
                        default=False, dest='very_verbose',
                        help='Be very verbose and write down all the queries')
    return parser


def main():
    """
    Reads a file and executes each line as a query
    to one or more elasticsearch indices
    Description:
    """
    start = default_timer()
    parser = _create_parser()
    args = parser.parse_args()
    print("Arguments:", args, file=sys.stderr, end='\n')
    gold_count = sum(args.batches)
    index = args.index
    gold_strategy = utils.FIELDS[args.goldstandard]
    k = args.at
    if '_all' in args.strategy:
        strategies = utils.FIELDS.values()
    else:
        strategies = [utils.FIELDS[strategy] for strategy in args.strategy]
    metrics = args.metric

    try:
        queryfile = open(args.querysource, 'r')
        if args.timestamp:
            tsrx = re.compile('\[[^\]]*\]\s(.*)')
            querystrings = [tsrx.match(line.strip()).group(1) for line in
                            queryfile.readlines()]
        else:
            querystrings = [line.strip() for line in queryfile.readlines()]

        docs_list = reduce_dicts(load_documents(args.documents))
        topic_list = reduce_dicts(load_documents(args.topics))

        gs_file = args.goldstandard
        goldstandard = np.array(pickle.load(open(gs_file)))
        # nullqueries = len([gold for gold in goldstandard if not len(gold)])
        # print("Warning:", nullqueries, "queries did not retrieve anything in\
        # the gold standard. Their score will always be 1")

        print("set up goldstandard with {} relevant documents per query\
              (batches: {})".format(gold_count, args.batches), file=sys.stderr)

        strategy_results = dict()
        for strategy in strategies:
            challenger = [utils.doc_ids(docs) for docs in
                          utils.execute_multisearch(index, strategy,
                                                    querystrings,
                                                    doc_type=args.doc_type,
                                                    size=k)]

            # results holds a dict of results for each metric
            results = perform_comparison(goldstandard, challenger, k,
                                         metrics,
                                         verbose=args.verbose)
            if args.very_verbose:
                query_results = zip(querystrings, results)
                fname = "_".join([args.metric, str(k), strategy]) + ".md"
                print_query_results(query_results, fname)
            strategy_results[strategy] = results

        # pretty printing
        for metric in metrics:
            print(file=args.output)
            print("# {}@{}".format(metric, str(k)), file=args.output)
            print(file=args.output)
            for strategy in strategies:
                metric_values = np.array(strategy_results[strategy][metric])
                print("| %s | %.4f (%.4f) |" % (strategy, metric_values.mean(),
                                                metric_values.std()),
                      file=args.output)

        queryfile.close()

    except FileNotFoundError:
        # use given string as raw query instead of a file
        # write out the retrieved documents' titles
        querystring = [str(args.querysource)]
        print("Query mode: ", querystring)
        fulltext = list(utils.perform_queries(args.gold_index,
                                              args.gold_doc_type, 'fulltext',
                                              gold_strategy,
                                              querystring, size=gold_count,
                                              source=True))[0]
        print("# Fulltext", args.goldstandard, file=args.output)
        for doc in fulltext:
            items = (doc['_score'], doc['_id'], doc['_source']['title'])
            print("", *items, file=args.output, sep="| ")
        print("# Titles", file=args.output)
        for strategy in args.strategy:
            titles = list(utils.perform_queries(index, args.doc_type,
                                                'title', strategy, querystring,
                                                size=k,
                                                source=True))[0]
            print("## Titles ", strategy, file=args.output)
            for doc in titles:
                items = (doc['_score'], doc['_id'], doc['_source']['title'])
                print("", *items, file=args.output, sep="| ")

    elapsed = default_timer() - start
    minutes, seconds = divmod(elapsed, 60)
    hours, minutes = divmod(minutes, 60)
    print("[querify] Finished after %d hours, %d minutes and %2.0f seconds."
          % (hours, minutes, seconds), file=sys.stderr)

if __name__ == '__main__':
    main()
