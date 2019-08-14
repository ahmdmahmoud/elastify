import sys
import pandas as pd
import numpy as np
from functools import reduce
try:
    import elastify.utils as utils
except ImportError:
    import utils


# def trainer(queryfile, index_doctype_prop, strategies, size=524287,
#             binary=False):
def trainer(queries, index, strategies, gold_index, gold_strategy,
            size=10000, batches=[5, 5, 5, 5], doctype=None):
    # index, doctype, prop = index_doctype_prop

    records = []
    gold_count = sum(batches)
    for qid, docs in enumerate(
            utils.execute_multisearch(gold_index, utils.FIELDS[gold_strategy],
                                      queries, size=gold_count,
                                      doc_type=doctype)):
        doc_relevance_pairs = utils.batched(batches, docs)
        for doc_relevance_pair in doc_relevance_pairs:
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
                utils.execute_multisearch(index, utils.FIELDS[strategy],
                                          queries, size=size,
                                          doc_type=doctype)):
            for doc in docs:
                print ("doc_title:", doc.meta.title, "doc")
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
        dfs.append(pd.DataFrame(records, columns=["qid", "did", strategy]))
        print()

    df = reduce(lambda left, right: pd.merge(left, right, how='outer',
                                             on=['qid', 'did']), dfs)
    df.fillna(0, inplace=True)
    print(df.columns)
    return df

def generate_DSSM_Data(queries, index, strategies, gold_index, gold_strategy,
            size=10000, batches=[5, 5, 5, 5], doctype=None):
    # index, doctype, prop = index_doctype_prop

    records = []
    gold_count = sum(batches)
    for qid, docs in enumerate(
            utils.execute_multisearch(gold_index, utils.FIELDS[gold_strategy],
                                      queries, size=gold_count,
                                      doc_type=doctype)):
        #print(queries[qid])
        #exit()
        doc_relevance_pairs = utils.batched(batches, docs)
        for doc_relevance_pair in doc_relevance_pairs:
            records.append({
                "query": queries[qid],
                "d_title": doc_relevance_pair[0].title,
                "qid": qid,
                gold_strategy: doc_relevance_pair[1], 
                
            })
            progress = 100 * (qid + 1) / len(queries)
            print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),
                                                progress),
                  flush=True, end='')
                  
    df = pd.DataFrame(records, columns=["query", "d_title"])
    gs =  pd.DataFrame(records, columns=["qid", gold_strategy])
    
    #dfs = [goldstandard]
    
    #df = reduce(lambda left, right: pd.merge(left, right, how='outer',
    #                                         on=['query', 'd_title']), dfs)
    #df.fillna(0, inplace=True)
    #print(df.columns)
    return df, gs
    
def generate_trainingdata(queryfile,
                          index_doctype_prop,
                          strategies,
                          size=None,
                          binary=False):
    """TODO: Docstring for generate_trainingdata.

    :queryfile: TODO
    :index: TODO
    :doctype: TODO
    :prop: TODO
    :strategies: TODO
    :size: TODO
    :binary: binarize the tfidf scores if it is a goalstanderd first parameter
    :returns: TODO

    """
    index, doctype, prop = index_doctype_prop
    queries = [querystring.strip() for querystring in queryfile.readlines()]
    records = []
    strategy_flag = 0
    for strategy in strategies:
        strategy_scores_temp = []
        print("Processing queries for", strategy)
        for qid, docs in enumerate(utils.perform_queries(index, doctype, prop,
                                                         strategy, queries,
                                                         size=size,
                                                         source=False)):
            for doc in docs:
                if strategy_flag == 0:
                    if binary == 'true' and strategy == 'tfidf':
                        doc['_score'] = 1.0 if doc['_score'] > 0.5 else 0.0 #convert to binary
                        records.append([doc['_score'],'qid:{}'.format(qid)])
                    else:
                        records.append([doc['_score'],'qid:{}'.format(qid)])

                else:
                    # add the scores from the second strategy
                    strategy_scores_temp.append('{}:{}'.format(strategy_flag,doc['_score']))

            progress = 100 * (qid + 1) / len(queries)
            print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),
                                                progress),
                  flush=True, end='')

        if strategy_flag != 0:
            records = np.c_[records, strategy_scores_temp]

        strategy_flag += 1
        print()

    df = pd.DataFrame(records, columns=["qid", *strategies]) #df = pd.DataFrame(records, columns=["qid", "doc_id", *strategies])
    #df = df.fillna(0)
    return df


def print_l2r(df, outfile):
    """formats the dataframe to obtain proper l2r training data

    :df: TODO
    :file: TODO
    :returns: TODO

    """
    for row in df.itertuples():
        # elems = [row[0], row[1]] + ['{}:{}'.format(i + 1, value) for i, value
        #                             in enumerate(row[2:])]

        # each row has the form [<index>, 'qid', 'did', 'metric1' ...]
        # strategies start at row[3]
        # row[0] : pandas internal index (do not use)
        # row[1] : query id
        # row[2] : document id
        # row[3] : first strategy: the gold standard
        # row[4:] : remaining strategies (on titles)
        # print(row)
        elems = [row[3],
            # 1 if row[3] > .5 else 0,
                 "qid:%d" % row[1]] + ['{}:{}'.format(i + 1, value) for i,
                                       value in enumerate(row[4:])] +\
            ["#doc_id:{}".format(row[2])]
        print(*elems, file=outfile)


        
def process_dssm_scores (gs, dssm_scores_file="../results/dssm_out.score.txt", cdssm_scores_file="../results/cdssm_out.score.txt",batches=[5, 5, 5, 5]):
    #You have to calculate the DSSM scores of the df output file first
    dssm_scores= pd.read_csv(dssm_scores_file, header=None,sep=r"\s+")
    cdssm_scores= pd.read_csv(cdssm_scores_file, header=None,sep=r"\s+")
    dssm_with_goldstanderd_df =  pd.concat([gs,dssm_scores], axis=1)
    #dssm_with_goldstanderd_df.to_csv("dssm_with_goldstanderd_df.txt ", sep='\t', header=False, index=False, mode='w')  
    dssm_ranked_df = dssm_with_goldstanderd_df.groupby('qid')[0].rank(ascending=False)
    dssm_ranked_df.to_csv("/home/asaleh/moving/Code/elastify/resultsdssm_ranked_df.txt ", sep='\t', header=False, index=False, mode='w')    
    
        
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
    parser.add_argument('-I', '--gold_index', default='economics',
                        help="The index for the gold standard ['economics']")
    parser.add_argument('-d', '--doctype', default='publication',
                        help="The document type to use ['publication']")
    parser.add_argument('-g', '--gold-strategy', default='fulltext',
                        help="The strateg{y,ies} to use. Default: 'fulltext'")
    parser.add_argument('-s', '--strategy', nargs='+', default=['tfidf'],
                        help="The strateg{y,ies} to use. Default: ['tfidf']",
                        choices=utils.FIELDS)
    parser.add_argument('-S', '--size', default=10000, type=int,
                        help="Number of documents per query for non-gold\
                        strategies. Default:10000")
    parser.add_argument('-B', '--batches', dest='batches', default=[5, 5, 5,
                                                                    5],
                        type=int,
                        nargs='+',
                        help="Specify batches as in '-B 5 5 5 5'")
    parser.add_argument('-o', '--outfile', default=sys.stdout,
                        type=argparse.FileType('w'),
                        help="Write output to outfile.")
    parser.add_argument('-T', '--type', default='dssm', type=str,
                        choices=['txt', 'l2r', 'csv', 'dssm'],
                        help='The type of the generated output.')
    args = parser.parse_args()
    #print(args)
    index, gold_index = args.index, args.gold_index
    strategies, gold_strategy = args.strategy, args.gold_strategy
    doctype = args.doctype
    size = args.size
    batches = args.batches
    queries = [querystring.strip() for querystring in
               args.queryfile.readlines()]
    size = args.size

    
    # print(df)

    if args.type == 'txt':
        df = trainer(queries, index, strategies, gold_index, gold_strategy,
                 size=size, batches=batches, doctype=doctype)
        df.to_csv(args.outfile, sep='\t', header=False, index=False, mode='w')
        exit(0)
    elif args.type == 'csv':
        df = trainer(queries, index, strategies, gold_index, gold_strategy,
                 size=size, batches=batches, doctype=doctype)
        df.to_csv(args.outfile, header=False, index=False, mode='a')
        exit(0)
    elif args.type == 'l2r':
        df = trainer(queries, index, strategies, gold_index, gold_strategy,
                 size=size, batches=batches, doctype=doctype)
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
