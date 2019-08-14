import sys
import pandas as pd
import numpy as np
import elastify.l2r_features.mk as mk
import elastify.l2r_features.semantic as sm
import elastify.l2r_features.letor as letor
import pickle
from os import listdir
from os.path import isfile, join
from functools import reduce
try:
    import elastify.utils as utils
except ImportError:
    import utils

def trainer(queries_dict, doc_dict, goldstandardfile, strategies,
                index='trec_titles', index_field='title', index_doctype='publication',
                lm_index='trec_fulltext', lm_index_field='fulltext', lm_index_doctype='publication',
                mu=10.0, w2v_model='', let_mu=2000.0, let_alpha=0.1, let_delta=0.7,
                size=10000, batches=[5, 5, 5, 5]):
    
    # for performance we cache some statistics
    total_term_freqs = {}
    
    features = []
    if "mk" in strategies:
        features.append(mk.MKFeatures(utils.client, ttfs=total_term_freqs, 
                                        index=index, index_field=index_field, doc_type=index_doctype,
                                        lm_index=lm_index, lm_index_field=lm_index_field, lm_doc_type=lm_index_doctype, mu=mu))
        strategies.remove("mk")
    if "sm" in strategies:
        features.append(sm.SemanticFeatures(w2v_modelfile=w2v_model))
        strategies.remove("sm")
    if "letor" in strategies:
        features.append(letor.LetorFeatures(utils.client, ttfs=total_term_freqs,
                                index=index, index_field=index_field, doc_type=index_doctype,
                                lm_index=lm_index, lm_index_field=lm_index_field, lm_doc_type=lm_index_doctype,
                                mu=let_mu, alpha=let_alpha, delta=let_delta))
        strategies.remove("letor")

    dfs_strategies = []

    for strategy in strategies:
        print('processing', strategy)
        records = []
        strat = utils.FIELDS[strategy]
        i = 0
        for query_id, query_text in queries_dict.items():
            docs = utils.execute_singlesearch(index, strat, query_text, size=size, doc_type=index_doctype)
        
            for doc in docs:

                records.append({
                    "qid": query_id,
                    "did": doc.meta.id,
                    strategy: doc.meta.score
                })
            #print(len(docs), "documents found for", query_id)

            progress = 100 * (i + 1) / len(queries_dict.items())
            i += 1
            print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),
                                                progress),
                  flush=True, end='')
            print()
        columns = ["qid", "did", strategy]
        
        tmp = pd.DataFrame(records,columns=columns)
        dfs_strategies.append(pd.DataFrame(records, columns=columns))

    print('number of qid,did-pairs', len(goldstandardfile))
    print('now processing l2r features:', features)
    records = []
    for (row_id, qid, did, goldscore) in goldstandardfile.itertuples():
        
        query_text = queries_dict[qid]
        doc_text = doc_dict[did]
        
        if not query_text:
            print('No query text for qid ', qid, 'of GS file')
            continue
        if not doc_text:
            print('No document text for did ', did, 'of GS file')
            continue
        record = {
            'gold': goldscore,
            'qid': qid,
            'did': did,
        }
        for f in features:
            values = f.calc_feature_values(query_text, doc_text)
            items = sorted(values.items(), key=lambda tup: tup[0])
            for key,value in items:
                record[key] = value
        records.append(record)
            
        #progress print
        progress = 100 * (row_id + 1) / len(goldstandardfile)
        print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),progress), flush=True, end='')
    print()
    columns = ['gold', "qid", "did"]
    for f in features:
        columns.extend(sorted(f.get_feature_names()))
    
    # here we merge the results of the different strategies coming from elastic with the goldstandard
    # we do a left join to preserve the goldstandard set 
    # from the docs:
    # Left outer join produces a complete set of records from Table A, with the matching records (where available) in Table B. 
    # If there is no match, the right side will contain null
    df = pd.DataFrame(records, columns=columns)
    for i in dfs_strategies:
        df = pd.merge(df, i, how='left', on=['qid', 'did'])
    
    # left merge can produce NaN values, so we have to eliminate them
    df.fillna(0, inplace=True)
    
    # change the order of columns for print_l2r function
    old_ordering = df.columns.tolist()
    old_ordering.remove('gold')
    new_ordering = ['gold'] + old_ordering
    df = df[new_ordering]
    print('columms:', df.columns.tolist())

    # RankLib doesn't sort the query ids by itself, so we do it beforehand
    df.sort_values(by='qid', axis=0, inplace=True)

    return df


def print_l2r(df, outfile):
    """formats the dataframe to obtain proper l2r training data

    :df: TODO
    :file: TODO
    :returns: TODO

    """
    tmp = ['{}:{}'.format(i + 1, name) for i,name in enumerate(df.columns.values.tolist()[3:])]
    print(tmp, file=outfile)
    for row in df.itertuples():
        # elems = [row[0], row[1]] + ['{}:{}'.format(i + 1, value) for i, value
        #                              in enumerate(row[2:])]

        # each row has the form [<index>, 'qid', 'did', 'metric1' ...]
        # strategies start at row[3]
        # row[0] : pandas internal index (do not use)
        # row[1] : gold strategy
        # row[2] : qid
        # row[3] : did
        # row[4:] : remaining strategies (on titles)
        # # print(row)
        # elems = [row[1],
        #     # 1 if row[3] > .5 else 0,
        #          "qid:%d" % row[2]] + ['{}:{}'.format(i + 1, value) for i,
        #                                value in enumerate(row[4:])] +\
        #     ["#doc_id:{}".format(row[3])]

        elems = [row[1],
            # 1 if row[3] > .5 else 0,
                 "qid:%s" % row[2]] + ['{}:{}'.format(i + 1, value) for i,
                                       value in enumerate(row[4:])] +\
            ["#doc_id:{}".format(row[3])]
        print(*elems, file=outfile)


        
def main():
    """Script to generate training data for learning 2 rank
    :returns: TODO

    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('queryfile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin)
    parser.add_argument('docsfile', nargs='?', type=argparse.FileType('r'))
    parser.add_argument('goldfile', nargs='?', type=str, default='')
    parser.add_argument('-i', '--index', default='trec_titles',
                        help="The index ['economics']")
    parser.add_argument('-f', '--field', default='title',
                        help="The index field ['title']")
    parser.add_argument('-d', '--doctype', default='publication',
                        help="The document type to use ['publication']")
    parser.add_argument('-s', '--strategy', nargs='+', default=['tfidf'],
                        help="The strateg{y,ies} to use. Default: ['tfidf']",
                        choices=utils.FIELDS)
    parser.add_argument('-S', '--size', default=10000, type=int,
                        help="Number of documents per query for non-gold\
                        strategies. Default:10000")
    parser.add_argument('-l', '--lindex', default='economics', 
                        help='The elasticsearch index for the language model (default: "economics")')
    parser.add_argument('-F', '--lfield', default='fulltext', 
                        help='The elasticsearch index field for the language model (default: "fulltext")')
    parser.add_argument('-B', '--batches', dest='batches', default=[5, 5, 5, 5],
                        type=int, nargs='+', help="Specify batches as in '-B 5 5 5 5'")
    parser.add_argument('-o', '--outfile', default=sys.stdout,
                        type=argparse.FileType('w'),
                        help="Write output to outfile.")
    parser.add_argument('-T', '--type', default='l2r', type=str,
                        choices=['txt', 'l2r'],
                        help='The type of the generated output.')
    parser.add_argument('-m', '--mu', default=10.0, type=float, help='The mu parameter for the mk language model score')
    parser.add_argument('-w', '--w2vmodel', default='/data5/commondata/L2R/GoogleNews-vectors-negative300.bin', type=str, help='The path to the mk word2vec model file')
    parser.add_argument('-x', '--let_mu', default=2000.0, type=float, help='The mu parameter for the letor language model with dirichlet smooting')
    parser.add_argument('-y', '--let_alpha', default=0.1, type=float, help='The alpha parameter for the letor language model with jelinek-mercer smoothing')
    parser.add_argument('-z', '--let_delta', default=0.7, type=float, help='The delta parameter for the letor language model with absolute discouting')
    args = parser.parse_args()
    #print(args)
    index = args.index
    lindex = args.lindex
    lfield = args.lfield
    strategies = args.strategy
    doctype = args.doctype
    size = args.size
    batches = args.batches
    
    # getting query text + ids
    queries_dict = dict()
    lines = args.queryfile.readlines()
    for line in lines:
        text = line.split('\t')
        qid, qtext = text[0], text[1].strip()
        queries_dict[qid] = qtext
    # getting doc text + ids
    doc_dict = {}
    lines = args.docsfile.readlines()
    for line in lines:
        elems = line.split('\t')
        doc_id, doc_text = elems[0], elems[1].strip()
        doc_dict[doc_id] = doc_text

    goldstandard = pd.read_csv(args.goldfile, sep = '\t', header = None, names = ["qid", "did", "rel"], dtype = str)
    print('shape:', goldstandard.shape)

    size = args.size
    if args.type == 'txt':
        df = trainer(queries_dict=queries_dict, doc_dict=doc_dict, goldstandardfile=goldstandard, 
                        index=index, index_field=args.field, index_doctype='publication',
                        lm_index=lindex, lm_index_field=lfield, lm_index_doctype='publication',
                        mu=args.mu, w2v_model=args.w2vmodel, let_mu=args.let_mu, let_alpha=args.let_alpha,let_delta=args.let_delta,
                        strategies=strategies, size=size, batches=batches)
        df.to_csv(args.outfile, sep='\t', header=False, index=False, mode='w')
        exit(0)
    elif args.type == 'l2r':
        df = trainer(queries_dict=queries_dict, doc_dict=doc_dict, goldstandardfile=goldstandard, 
                        index=index, index_field=args.field, index_doctype=doctype,
                        lm_index=lindex, lm_index_field=lfield, lm_index_doctype='publication',
                        mu=args.mu, w2v_model=args.w2vmodel, let_mu=args.let_mu, let_alpha=args.let_alpha,let_delta=args.let_delta,
                        strategies=strategies, size=size, batches=batches)
        print_l2r(df, args.outfile)
        exit(0)
    else:
        print("Desired type not available", file=sys.stderr)
        exit(-1)


if __name__ == "__main__":
    main()
