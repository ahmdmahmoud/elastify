import sys
import pandas as pd
import numpy as np
import elastify.l2r_features.mk as mk
import elastify.l2r_features.semantic as sm
import elastify.l2r_features.letor as letor
from functools import reduce
try:
    import elastify.utils as utils
except ImportError:
    import utils

# ordered score (between 0 and length of list -1)
def sort_replace(pairs, reverse=False):
    sorted_pairs = sorted(pairs, key=lambda k: k[0].meta.score, reverse=reverse)
    if reverse:
        count = 0
        for i in sorted_pairs:
            i[0].meta.score = count
            count += 1
    else:   
        count = len(sorted_pairs)-1
        for i in sorted_pairs:
            i[0].meta.score = count
            count -= 1
    return sorted_pairs  

# multiply score by 10 and then round (score between 0 and 1)
def mult_round(pairs):
    # have to create copy as class 'zip' allows no in-memory changes
    sorted_pairs = list(pairs)
    for pair in sorted_pairs:
        pair[0].meta.score = round(pair[0].meta.score * 10)
    return sorted_pairs

# if score is > 0.5 then it's relevant -> 1
def binary(pairs):
    cpy = list(pairs)
    for c in cpy:
        if c[0].meta.score > 0.5: 
            c[0].meta.score = 1
        else:
            c[0].meta.score = 0
    return cpy

def trainer(queries, index, strategies, gold_index, gold_strategy,
                index_field='title', index_doctype='publication',
                lm_index='trec_fulltext', lm_index_field='fulltext', lm_index_doctype='publication',
                w2v_model='', mu=10.0, let_mu=2000.0, let_alpha=0.1, let_delta=0.7,
                size=10000, batches=[5, 5, 5, 5]):
    
    doc_id_map = {}
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

    print('catch gold standard')
    records = []
    gold_count = sum(batches)
    for qid, docs in enumerate(
            utils.execute_multi_singlesearch(gold_index, utils.FIELDS[gold_strategy],
                                      queries, size=gold_count,
                                      doc_type=index_doctype)):

        doc_relevance_pairs = utils.batched(batches, docs)
        for doc_relevance_pair in doc_relevance_pairs:
            ## doc_relevance_pair = {index: economics, id: 1000235130, score: 4.3025, doc_type: publication}
            doc_id_map[doc_relevance_pair[0].meta.id] = doc_relevance_pair[0].title

            records.append({
                "qid": qid,
                "did": doc_relevance_pair[0].meta.id,
                gold_strategy: doc_relevance_pair[1]
            })

    goldstandard = pd.DataFrame(records, columns=["qid", "did", gold_strategy])
    
    dfs = [goldstandard]
    for strategy in strategies:
        print('processing', strategy)
        records = []
        for qid, docs in enumerate(
                utils.execute_multi_singlesearch(index, utils.FIELDS[strategy],
                                          queries, size=size,
                                          doc_type=index_doctype)):
            for doc in docs:
                doc_id_map[doc.meta.id] = doc.title
                records.append({
                    "qid": qid,
                    "did": doc.meta.id,
                    strategy: doc.meta.score
                })
            #print(len(records), "documents found for", qid)

            progress = 100 * (qid + 1) / len(queries)
            print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),
                                                progress),
                  flush=True, end='')
            print()
        dfs.append(pd.DataFrame(records, columns=["qid", "did", strategy]))
        #print()

    print('reduce and merge retrieval records')
    df = reduce(lambda left, right: pd.merge(left, right, how='outer',
                                             on=['qid', 'did']), dfs)
    df.fillna(0, inplace=True)

    # index, doctype, prop = index_doctype_prop
    #params for language model score
    if features:
        print('append feature columns')
        feature_names = []
        for f in features:
            feature_names.extend(sorted(f.get_feature_names()))
        for f in feature_names:
            df[f] = 0.0
        print('calculate external feature values')
        length, _ = df.shape
        for (i, qid, did, *_) in df.itertuples():
            text = doc_id_map[did]
            query = queries[qid]
            for f in features:
                result = f.calc_feature_values(query, text)
                for key,value in result.items():
                    df.loc[i,key] = value
            sys.stdout.write('Progress: {}  \r'.format(i/length*100))
            sys.stdout.flush()

        print('Calculated features for {} instances'.format(length) )

    df.sort_values(by='qid', axis=0, inplace=True)
    return df


    
def trainer_fulltext(queries, index, strategies, gold_index, gold_strategy,
                index_field='fulltext', index_doctype='publication',
                lm_index='trec_fulltext', lm_index_field='fulltext', lm_index_doctype='publication',
                w2v_model='', mu=10.0, let_mu=2000.0, let_alpha=0.1, let_delta=0.7,
                size=10000, batches=[5, 5, 5, 5]):
    
    doc_id_map = {}
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

    print('catch gold standard')
    records = []
    gold_count = sum(batches)
    for qid, docs in enumerate(
            utils.execute_multi_singlesearch(gold_index, utils.FIELDS[gold_strategy],
                                      queries, size=size,
                                      doc_type=index_doctype, test_subject=True)):

        #doc_relevance_pairs = utils.batched(batches, docs)
        for doc_relevance_pair in docs:
            ## doc_relevance_pair = {index: economics, id: 1000235130, score: 4.3025, doc_type: publication}
            doc_id_map[doc_relevance_pair[0].meta.id] = doc_relevance_pair[0].fulltext

            records.append({
                "qid": qid,
                "did": doc_relevance_pair[0].meta.id,
                gold_strategy: doc_relevance_pair[1]
            })

    goldstandard = pd.DataFrame(records, columns=["qid", "did", gold_strategy])
    
    dfs = [goldstandard]
    for strategy in strategies:
        print('processing', strategy)
        records = []
        for qid, docs in enumerate(
                utils.execute_multi_singlesearch(index, utils.FIELDS[strategy],
                                          queries, size=size,
                                          doc_type=index_doctype)):
            for doc in docs:
                doc_id_map[doc.meta.id] = doc.fulltext
                records.append({
                    "qid": qid,
                    "did": doc.meta.id,
                    strategy: doc.meta.score
                })
            #print(len(records), "documents found for", qid)

            progress = 100 * (qid + 1) / len(queries)
            print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),
                                                progress),
                  flush=True, end='')
            print()
        dfs.append(pd.DataFrame(records, columns=["qid", "did", strategy]))
        #print()

    print('reduce and merge retrieval records')
    df = reduce(lambda left, right: pd.merge(left, right, how='outer',
                                             on=['qid', 'did']), dfs)
    df.fillna(0, inplace=True)

    # index, doctype, prop = index_doctype_prop
    #params for language model score
    if features:
        print('append feature columns')
        feature_names = []
        for f in features:
            feature_names.extend(sorted(f.get_feature_names()))
        for f in feature_names:
            df[f] = 0.0
        print('calculate external feature values')
        length, _ = df.shape
        for (i, qid, did, *_) in df.itertuples():
            text = doc_id_map[did]
            query = queries[qid]
            for f in features:
                result = f.calc_feature_values(query, text)
                for key,value in result.items():
                    df.loc[i,key] = value
            sys.stdout.write('Progress: {}  \r'.format(i/length*100))
            sys.stdout.flush()

        print('Calculated features for {} instances'.format(length) )

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
    #print(df.columns.values.tolist(), file=outfile)
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

        elems = [row[3],
            # 1 if row[3] > .5 else 0,
                 "qid:%d" % row[1]] + ['{}:{}'.format(i + 1, value) for i,
                                       value in enumerate(row[4:])] +\
            ["#doc_id:{}".format(row[2])]
        print(*elems, file=outfile)

    
def main():
    """Script to generate training data for learning 2 rank
    :returns: TODO

    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('queryfile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin)
    parser.add_argument('-i', '--index', default='economics',
                        help="The index ['economics']")
    parser.add_argument('-f', '--field', default='title',
                        help="The field for the index ['title']")
    parser.add_argument('-d', '--doctype', default='publication',
                        help="The document type to use ['publication']")
    parser.add_argument('-g', '--gold-strategy', default='fulltext',
                        help="The strateg{y,ies} to use. Default: 'fulltext'")
    parser.add_argument('-I', '--gold_index', default='economics',
                        help="The index for the gold standard ['economics']")
    parser.add_argument('-s', '--strategy', nargs='+', default=['tfidf'],
                        help="The strateg{y,ies} to use. Default: ['tfidf']",
                        choices=utils.FIELDS)
    parser.add_argument('-S', '--size', default=10000, type=int,
                        help="Number of documents per query for non-gold\
                        strategies. Default:10000")
    parser.add_argument('-l', '--lindex', default='economics', 
                        help='The elasticsearch index for the language model ["economics"]')
    parser.add_argument('-F', '--lfield', default='fulltext', 
                        help='The elasticsearch index field for the language model (default: "fulltext")')
    parser.add_argument('-B', '--batches', dest='batches', default=[5, 5, 5, 5],
                        type=int, nargs='+',
                        help="Specify batches as in '-B 5 5 5 5'")
    parser.add_argument('-o', '--outfile', default=sys.stdout,
                        type=argparse.FileType('w'),
                        help="Write output to outfile.")
    parser.add_argument('-T', '--type', default='dssm', type=str,
                        choices=['txt', 'l2r', 'csv', 'dssm', 'l2rfulltext'],
                        help='The type of the generated output.')
    parser.add_argument('-m', '--mu', default=10.0, type=float, help='The mu parameter for the mk language model score')
    parser.add_argument('-w', '--w2vmodel', default='/data3/tbeck/data/GoogleNews-vectors-negative300.bin', type=str, help='The path to the mk word2vec model file')
    parser.add_argument('-x', '--let_mu', default=2000.0, type=float, help='The mu parameter for the letor language model with dirichlet smooting')
    parser.add_argument('-y', '--let_alpha', default=0.1, type=float, help='The alpha parameter for the letor language model with jelinek-mercer smoothing')
    parser.add_argument('-z', '--let_delta', default=0.7, type=float, help='The delta parameter for the letor language model with absolute discouting')
    args = parser.parse_args()
    #print(args)
    index, gold_index = args.index, args.gold_index
    strategies, gold_strategy = args.strategy, args.gold_strategy
    doctype = args.doctype
    field = args.field
    lindex = args.lindex
    lfield = args.lfield
    size = args.size
    batches = args.batches
    queries = [querystring.strip() for querystring in
               args.queryfile.readlines()]
    size = args.size

    
    # print(df)

    if args.type == 'txt':
        df = trainer(queries, index, strategies, gold_index, gold_strategy,
                index_field=field, index_doctype=doctype,
                lm_index=lindex, lm_index_field=lfield, lm_index_doctype='publication',
                mu=10.0, w2v_model=args.w2vmodel, let_mu=2000.0, let_alpha=0.1, let_delta=0.7,
                size=size, batches=batches)
        df.to_csv(args.outfile, sep='\t', header=False, index=False, mode='w')
        exit(0)
    elif args.type == 'csv':
        df = trainer(queries, index, strategies, gold_index, gold_strategy,
                index_field=field, index_doctype=doctype,
                lm_index=lindex, lm_index_field=lfield, lm_index_doctype='publication',
                mu=10.0, w2v_model=args.w2vmodel, let_mu=2000.0, let_alpha=0.1, let_delta=0.7,
                size=size, batches=batches)
        df.to_csv(args.outfile, header=False, index=False, mode='a')
        exit(0)
    elif args.type == 'l2r':
        df = trainer(queries, index, strategies, gold_index, gold_strategy,
                index_field=field, index_doctype=doctype,
                lm_index=lindex, lm_index_field=lfield, lm_index_doctype='publication',
                mu=10.0, w2v_model=args.w2vmodel, let_mu=2000.0, let_alpha=0.1, let_delta=0.7,
                size=size, batches=batches)
        print_l2r(df, args.outfile)
        exit(0)
        
    elif args.type == 'l2rfulltext':
        df = trainer_fulltext(queries, index, strategies, gold_index, gold_strategy,
                index_field=field, index_doctype=doctype,
                lm_index=lindex, lm_index_field=lfield, lm_index_doctype='publication',
                mu=10.0, w2v_model=args.w2vmodel, let_mu=2000.0, let_alpha=0.1, let_delta=0.7,
                size=size, batches=batches)
        print_l2r(df, args.outfile)
        exit(0)
    if args.type == 'dssm':
        df, gs = generate_DSSM_Data(queries, index, strategies, gold_index, gold_strategy,
                 size=size, batches=batches, doctype=doctype)
        df.to_csv(args.outfile, sep='\t', header=False, index=False, mode='w')
        #gs.to_csv('gs-'+str(args.outfile.name), sep='\t', header=False, index=False, mode='w')
        process_dssm_scores(gs)
        
        exit(0)
    else:
        print("Desired type not available", file=sys.stderr)
        exit(-1)


if __name__ == "__main__":
    main()
