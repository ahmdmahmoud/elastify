"""
 querify4labeled  resources_tmp/ntcir-veryshortqueries.txt resources_tmp/rel1_ntc2-e2_0101-0149.nc -i ntcir_titles -s tfidf   -o results/querify4labeledtest111.txt -T csv
 querify4labeled  resources_tmp/trec_topics.txt resources_tmp/qrels_trec6_adhoc_all.txt -i trec_titles -s tfidf   -o results/querify4labeledtest222.txt -T csv
Example for running the code to generate results with the ntcir dataset. 
please note that the queries in your ntcir-veryshortqueries file, should contain the same match the number of queries in rel1_ntc2-e2_0101-0149.nc. Same order is required. 
rels should be in the fourth col in the rels file. 
 """

import sys
import pandas as pd
import numpy as np
import decimal
from functools import reduce
try:
    import elastify.utils as utils
    import elastify.rank_metrics as rm
except ImportError:
    import rank_metrics as rm
    import utils



def generate_doc_strategy_labels_data(queries, rels_scores, index, strategies,  
            size=10000, doctype=None, qids=[]):
    # index, doctype, prop = index_doctype_prop

    #gold_count = sum(batches)
    
    if "economics" in index or "politics" in index or "pubmed" in index or "bm25" in index:
        subjects_test_bool = True
    else:
        subjects_test_bool = False
    results = []
    for strategy in strategies:
        j = 0
        records = []
        print('processing', strategy)
        #queries = ['"{0}"'.format(q) for q in queries]  #add quotation marks to querystrings
        for qid, docs in enumerate(
                utils.execute_multi_singlesearch(index, utils.FIELDS[strategy],
                                          queries, size=size,
                                          doc_type=doctype, test_subject=subjects_test_bool)):
            if subjects_test_bool:
                new_qid = qids[j]
                j+=1
            else:
                #print (qid)
                #print (int(rels_scores.iloc[0].loc["new_qid"]))
                #doc_relevance_pairs = utils.batched(batches, docs)
                new_qid = qid + int(rels_scores.iloc[0].loc["new_qid"]) # adapts the ids if the rels files doesnot start with id of zero 
                #new_qid = qid + int(rels[0]['new_qid']) # adapts the ids if the rels files doesnot start with id of zero 
            print(len(docs), "documents found for query number", new_qid)

            for doc in docs:
                #print (doc)
                #exit()
                records.append({
                    "new_qid": new_qid,
                    "did": doc.meta.id,
                    "rel": doc.meta.score
                })
                
                progress = 100 * (qid + 1) / len(queries)
                print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),
                                                    progress),
                      flush=True, end='')

        #print (len(records), records[0])
        strategy_scores = pd.DataFrame(records, columns=["new_qid", "did", "rel"])
        #strategy_scores.to_csv("strategy.txt", sep=' ', header=False, index=False, mode='w')
        
        #print (len(strategy_scores), strategy_scores.loc[strategy_scores.index[0], 'rel'])
        #dfs = [strategy_scores]
        
        #rels_scores = pd.DataFrame(rels, columns=["new_qid", "did", "gold_rel_score"])
        #rels_scores.to_csv("rels.txt", sep=' ', header=False, index=False, mode='w')

        if "economics" in index or "politics" in index or "pubmed" in index:
            df = pd.merge(strategy_scores, rels_scores, how='left', on=['new_qid', 'did']) #merge the scores from the relevancy file and the strategy
        else:
            df = pd.merge(strategy_scores, rels_scores, how='right', on=['new_qid', 'did']) #merge the scores from the relevancy file and the strategy

        df.fillna(0, inplace=True)
        #df.to_csv("test4.txt", sep=' ', header=False, index=False, mode='w')

        result = evaluate_labeled_data ( df )  
        results.append({"index": index,
                    "metric": "ndcg@20",
                    "strategy": strategy,
                    "ndcg": result})
                    
        #print ("For: ", strategy, "nDCG@20=", result)
        
    return pd.DataFrame(results, columns=["index", "strategy", "metric", "ndcg"])

def evaluate_labeled_data (labeled_data, k=20) : 
    gp = labeled_data.groupby(['new_qid']) 
    ndcgs = []
    for qid in gp.groups:
        labeled_data_for_qid = labeled_data[labeled_data['new_qid'] == qid]
        challenger_rel = labeled_data_for_qid.sort_values(by= ['rel'], ascending=False)['gold_rel_score']
        #print ("Challenger ", challenger_rel)
        #print (labeled_data_for_qid.sort_values(by= ['gold_rel_score'], ascending=False)['rel'])
        #print ("NDCG: ", rm.ndcg_at_k(challenger_rel, k=k))
        ndcgs.append( rm.ndcg_at_k(challenger_rel, k=k) )

    print(ndcgs)   
    final_scores = [0 if float(len(ndcgs))==0 else sum(ndcgs)/float(len(ndcgs))] #if there is no documents match with the retrieval model 
    print (final_scores)
    return final_scores

def generate_qrels_file_for_digital_lib_data(queries, index, relsoutfile, size=10000, doctype=None):

    records = []
    print('generating qrels file from subject field...')
    for qid, docs in enumerate(
            utils.execute_multi_singlesearch(index, utils.Strategy(["subject"], index=index),  #utils.FIELDS["subject"],
                                      queries, size=size,
                                      doc_type=doctype)):
        print(len(docs), "documents found for query number (qrels file generation)", qid)

        for doc in docs:
            records.append({
                "new_qid": qid,
                "did": doc.meta.id,
                "gold_rel_score": 1
            })
            
        progress = 100 * (qid + 1) / len(queries)
        print('\r[{0:10}] {1:3.0f}%'.format("#" * int(progress//10),
                                            progress),
              flush=True, end='')

    records_df = pd.DataFrame(records, columns=["new_qid", "did", "gold_rel_score"])
    records_df.to_csv(relsoutfile, sep=' ', header=False, index=False, mode='w')
    return records_df 

    
    
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


def main():
    """Script to generate training data for learning 2 rank
    :returns: TODO

    """
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('queryfile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin)
    parser.add_argument('relfile', nargs='?', type=argparse.FileType('r'),
                        default=sys.stdin)
    parser.add_argument('-i', '--index', default='ntcir_titles',
                        help="The index ['ntcir_titles']")
    parser.add_argument('-d', '--doctype', default='publication',
                        help="The document type to use ['publication']")
    parser.add_argument('-s', '--strategy', nargs='+', default=['tfidf'],
                        help="The strateg{y,ies} to use. Default: ['tfidf']",
                        choices=utils.FIELDS)
    parser.add_argument('-S', '--size', default=1000, type=int,
                        help="Number of documents per query for non-gold\
                        strategies. Default:10000")
    parser.add_argument('-B', '--batches', dest='batches', default=[5, 5, 5,
                                                                    5],
                        help="Specify batches as in '-B 5 5 5 5'")
    parser.add_argument('-o', '--outfile', default=sys.stdout,
                        type=argparse.FileType('w'),
                        help="Write output to outfile.")
    parser.add_argument('-T', '--type', default='l2r', type=str,
                        choices=['txt', 'l2r', 'csv', 'txt_ss'],
                        help='The type of the generated output.')
    parser.add_argument('-ro', '--relsoutfile',
                        type=argparse.FileType('w'),
                        help="Generate and write rel scores to relsoutfile.")
    args = parser.parse_args()
    print(args)
    index  = args.index
    strategies = args.strategy
    doctype = args.doctype
    size = args.size
    #batches = args.batches
    queries = [querystring.strip() for querystring in
               args.queryfile.readlines()]
    
    
    if "ntcir" in index: 
        rels = [( {"new_qid": decimal.Decimal(int(relstring.strip().split("\t")[0])), "did": relstring.strip().split("\t")[2],  "gold_rel_score": decimal.Decimal(int(relstring.strip().split("\t")[3]))} ) for relstring in
               args.relfile.readlines()] #ntcir
        rels_scores = pd.DataFrame(rels, columns=["new_qid", "did", "gold_rel_score"])
        df = generate_doc_strategy_labels_data(queries, rels_scores,  index, strategies, size=size, doctype=doctype)
                 
    elif "trec" in index:  
        rels = [( {"new_qid": decimal.Decimal(int(relstring.strip().split(" ")[0])), "did": relstring.strip().split(" ")[2],  "gold_rel_score": decimal.Decimal(int(relstring.strip().split(" ")[3]))} ) for relstring in
               args.relfile.readlines()] #trec
        rels_scores = pd.DataFrame(rels, columns=["new_qid", "did", "gold_rel_score"])
        df = generate_doc_strategy_labels_data(queries, rels_scores,  index, strategies, size=size, doctype=doctype)
    
    elif "economics" in index or "politics" in index or "pubmed" in index or "bm25" in index:
        if args.relsoutfile is not None:
            rels_scores = generate_qrels_file_for_digital_lib_data(queries, index, args.relsoutfile, size=size, doctype=doctype)
        else:
            rels = [( {"new_qid": decimal.Decimal(int(relstring.strip().split(" ")[0])), "did": relstring.strip().split(" ")[1],  "gold_rel_score": decimal.Decimal(int(relstring.strip().split(" ")[2]))} ) for relstring in
               args.relfile.readlines()] 
            rels_scores = pd.DataFrame(rels, columns=["new_qid", "did", "gold_rel_score"])
        #df = generate_qrels_file_for_digital_lib_data(queries, index, strategies, size=size, doctype=doctype)
        qids = rels_scores.new_qid.unique()
        new_queries = []
        new_qids = []
        for qid in qids:
            i = int(qid)
            new_qids.append(i)
            new_queries.append(queries[i])
        queries = new_queries
        qids = new_qids

        #print ("QIDS ", qids)
        df = generate_doc_strategy_labels_data(queries, rels_scores, index, strategies, size=size, doctype=doctype, qids=qids)
    
    else: 
        print ('We can not read the relevancy scores file of your index')
    
    
    if args.type == 'txt':
        df.to_csv(args.outfile, sep='\t', header=False, index=False, mode='a')
        exit(0)
    elif args.type == 'csv':
        df.to_csv(args.outfile, header=False, index=False, mode='a')
        exit(0)
    elif args.type == 'l2r':
        print_l2r(df, args.outfile)
        exit(0)
    elif args.type == 'txt_ss':
        df.to_csv(args.outfile, sep=' ', header=False, index=False, mode='a')
        exit(0)
    else:
        print("Desired type not available", file=sys.stderr)
        exit(-1)


if __name__ == "__main__":
    main()
