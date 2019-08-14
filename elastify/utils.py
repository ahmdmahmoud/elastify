"""
Some utils for the elastify package
"""
import sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, MultiSearch, Q
from elasticsearch_dsl.query import Bool, Query
from collections import defaultdict

client = Elasticsearch([{"host": "localhost"}], timeout=3600)

DEBUG = False


class SemanticQuery(Query):

    """Support for Semantic Query Parser"""
    name = 'semantic'


class Strategy(object):
    """Strategy object composed of plain text and semantic fields"""

    def __init__(self, match, semantic=None, index='economics',  prefix=None, boost=None):
        """Initializes a strategy object

        :index to be searched on
        :match: set of fields to be searched as match queries
        :semantic: set of fields to be searched as semantic queries

        """
        self._index = index
        self._match = match
        self._semantic = semantic if semantic else []
        self._prefix = prefix
        self._boost = boost if boost else defaultdict(lambda: 1)

    def prefix(self, prefix):
        return Strategy(self._match, semantic=self._semantic, index=self._index, prefix=prefix,
                        boost=self._boost)

    def boost(self, key, value):
        # TODO make copy deep
        boost = defaultdict(lambda: 1, self._boost)  # copy
        boost[key] = value
        return Strategy(self._match, semantic=self._semantic, index=self._index, 
                        prefix=self._prefix, boost=boost)

    def _prefixed(self, field):
        return ".".join([self._prefix, field]) if self._prefix else field

    def get_index(self):
        return self._index


    def semantic_query(self, term):
        # TODO add boost sensitivity
        """ This is how you create complex bool queries xd"""
        clauses = [Q({"match":
                      {self._prefixed(field):
                       {"query": term,
                        "boost": self._boost[field]
                        # "fuzziness":0,
                        # "fuzzy_transpositions": False
                        }
                       }
                      }
                     )
                   for field in self._match]
        
        clauses.extend([Q({"semantic":
                           {self._prefixed(field):
                            {"term": term, 
                            "boost": self._boost[field]
                            }}})
                        for field in self._semantic])
        #print(clauses)
        
        q = Bool(should=clauses)

        if DEBUG:
            print(q.to_dict())
        return q

    def semantic_query_test(self, term):
        # TODO add boost sensitivity
        """ This is how you create complex bool queries xd"""
        clauses = [Q({"match":
                      {self._prefixed(field):
                       {"query": term,
                        "boost": 1,
                        "type": "phrase"
                        # "fuzziness":0,
                        # "fuzzy_transpositions": False
                        }
                       }
                      }
                     )
                     for field in self._match]
                     
        clauses.extend( [Q({"exists": {
                            "field": "subject"
                        }})]) 
        
        #clauses.extend([Q({"semantic":
        #                   {self._prefixed(field):
        #                    {"term": term, 
        #                    "boost": self._boost[field]
        #                    }}})
        #                for field in self._semantic])
        #print(clauses)
        
        q = Bool(must=clauses)

        if DEBUG:
            print(q.to_dict())
        return q

    def query_body(self, term):
        # TODO add boost sensitivity
        # {'bool': {'should': [{'multi_match': {'fields': ['title.CFIDF',
        #                                                  'title.TFIDF'],
        #                                       'type': 'most_fields', 'query':
        #                                       'kiribati'}}, {'term':
        #                                                      {'title.HFIDF':
        #                                                       'kiribati'}}]}}

        clauses = [{"match":
                    {self._prefixed(field):
                     {"value": term,
                      "boost": self._boost[field]
                      }
                     }
                    } for field in self._match]

        clauses.extend([{"semantic":
                         {self._prefixed(field):
                          {"value": term,
                           "boost": self._boost[field]
                           }
                          }
                         } for field in self._semantic])

        q = {"query": {"bool": {"should": clauses}}}
        return q

    def __str__(self):
        def boost_indicator(field):
            return field if self._boost[field] == 1 else "{}^{}".format(field, self._boost[field])
        fields = self._match + self._semantic
        return ",".join([boost_indicator(field) for field in fields])


def doc_id(doc):
    try:
        return doc.meta.id
    except AttributeError:
        return doc['_id']


def doc_ids(docs):
    return [doc_id(d) for d in docs]


def batched(batches, docs):
    """Splits the documents into relevancy batches

    :batches: list of desired batches such as [5,5,5,5]
    :docs: Iterable of (hashable) documents
    :returns: zipped documents with their batch relevance score

    """
    rmax = len(batches)
    mask = list()
    for i, batch in enumerate(batches):
        mask.extend([rmax - i] * batch)
    return zip(docs, mask)
    # batch_relevancy = defaultdict(int, zip(docs, mask))
    # return batch_relevancy


# FIELDS for the indices in elasticsearch v 2.3
FIELDS =  {"fulltext": Strategy(["fulltext"], index="economics"),
           "economics-subjects": Strategy(["subject"], index="economics"),
          "tfidf": Strategy(["TFIDF"], index="economics").prefix("title"),
          "cfidf": Strategy(["CFIDF"], index="economics").prefix("title"),
          "ctfidf": Strategy(["TFIDF", "CFIDF"], index="economics").prefix("title"),
          "hfidf": Strategy([], ["HFIDF"], index="economics").prefix("title"),
          "hcfidf": Strategy(["CFIDF"], ["HFIDF"], index="economics").prefix("title"),
          "hctfidf": Strategy(["CFIDF", "TFIDF"], ["HFIDF"], index="economics").prefix("title"),
          "ohidf": Strategy(["OHIDF"], index="onehop").prefix("title"),
          "tohidf": Strategy(["TFIDF", "OHIDF"], index="onehop").prefix("title"),
          "bm25": Strategy(["BM25"], index="bm25titles").prefix("title"),
          "bm25c": Strategy(["BM25C"], index="bm25titles").prefix("title"),
          "bm25ct": Strategy(["BM25", "BM25C"], index="bm25titles").prefix("title"),
          "bm25h": Strategy([],["BM25H"], index="bm25titles").prefix("title"),
          "bm25hc": Strategy(["BM25C"], ["BM25H"], index="bm25titles").prefix("title"),
          "bm25hct": Strategy(["BM25", "BM25C"], ["BM25H"], index="bm25titles").prefix("title"),
          "bm25oh": Strategy(["BM25OH"],index="onehop").prefix("title"),
          "bm25oht": Strategy(["BM25", "BM25OH"],index="onehop").prefix("title"),
          "boosts": Strategy(["BM25", "BM25C"],
                              ["BM25H"], index="bm25titles").prefix("title").boost("BM25C", 100),
          "tfidf-full": Strategy(["TFIDF"], index='economics').prefix("fulltext"),
          "cfidf-full": Strategy(["CFIDF"], index='economics').prefix("fulltext"),
          "ctfidf-full": Strategy(["TFIDF", "CFIDF"], index='economics').prefix("fulltext"),
          "hfidf-full": Strategy([], ["HFIDF"], index='economics').prefix("fulltext"),
          "hcfidf-full": Strategy(["CFIDF"], ["HFIDF"], index='economics').prefix("fulltext"),
          "hctfidf-full": Strategy(["CFIDF", "TFIDF"], ["HFIDF"], index='economics').prefix("fulltext"),
          "ohidf-full": Strategy(["OHIDF"], index='onehop').prefix("fulltext"),
          "tohidf-full": Strategy(["TFIDF", "OHIDF"], index='onehop').prefix("fulltext"),
          "bm25-full": Strategy(["BM25"], index='bm25full').prefix("fulltext"),
          "bm25c-full": Strategy(["BM25C"], index='bm25full').prefix("fulltext"),
          "bm25ct-full": Strategy(["BM25", "BM25C"], index='bm25full').prefix("fulltext"),
          "bm25h-full": Strategy([], ["BM25H"], index='bm25full').prefix("fulltext"),
          "bm25hc-full": Strategy(["BM25C"], ["BM25H"], index='bm25full').prefix("fulltext"),
          "bm25hct-full": Strategy(["BM25", "BM25C"], ["BM25H"], index='bm25full').prefix("fulltext"),
          "bm25oh-full": Strategy(["BM25OH"], index='bm25full').prefix("fulltext"),
          "bm25oht-full": Strategy(["BM25", "BM25OH"], index='bm25full').prefix("fulltext"),
          "boosts-full": Strategy(["BM25", "BM25C"],
                              ["BM25H"], index='bm25full').prefix("title").boost("BM25C", 100),
          
          "pubmed-fulltext": Strategy(["fulltext"], index="pubmed_final"),
          "pubmed-subjects": Strategy(["subject"], index="pubmed_final"),
          "pubmed-tfidf": Strategy(["TFIDF"], index="pubmed_final").prefix("title"),
          "pubmed-cfidf": Strategy(["CFIDF"], index="pubmed_final").prefix("title"),
          "pubmed-ctfidf": Strategy(["TFIDF", "CFIDF"], index="pubmed_final").prefix("title"),
          "pubmed-hfidf": Strategy([], ["HFIDF"], index="pubmed_final").prefix("title"),
          "pubmed-hcfidf": Strategy(["CFIDF"], ["HFIDF"], index="pubmed_final").prefix("title"),
          "pubmed-hctfidf": Strategy(["CFIDF", "TFIDF"], ["HFIDF"], index="pubmed_final").prefix("title"),
          "pubmed-bm25": Strategy(["BM25"], index="pubmed_final").prefix("title"),
		  "pubmed-bm25c": Strategy(["BM25C"], index="pubmed_final").prefix("title"),
          "pubmed-bm25ct": Strategy(["BM25", "BM25C"], index="pubmed_final").prefix("title"),
          
          "pubmed-fulltext-f": Strategy(["fulltext"], index="pubmed_final"),
          "pubmed-subjects-f": Strategy(["subject"], index="pubmed_final"),
          "pubmed-tfidf-f": Strategy(["TFIDF"], index="pubmed_final").prefix("fulltext"),
          "pubmed-cfidf-f": Strategy(["CFIDF"], index="pubmed_final").prefix("fulltext"),
          "pubmed-ctfidf-f": Strategy(["TFIDF", "CFIDF"], index="pubmed_final").prefix("fulltext"),
          "pubmed-hfidf-f": Strategy([], ["HFIDF"], index="pubmed_final").prefix("fulltext"),
          "pubmed-hcfidf-f": Strategy(["CFIDF"], ["HFIDF"], index="pubmed_final").prefix("fulltext"),
          "pubmed-hctfidf-f": Strategy(["CFIDF", "TFIDF"], ["HFIDF"], index="pubmed_final").prefix("fulltext"),
		  "pubmed-bm25-f": Strategy(["BM25"], index="pubmed_final").prefix("fulltext"),
          "pubmed-bm25c-f": Strategy(["BM25C"], index="pubmed_final").prefix("fulltext"),
          "pubmed-bm25ct-f": Strategy(["BM25", "BM25C"], index="pubmed_final").prefix("fulltext"),
          
		  
          "politics-fulltext": Strategy(["fulltext"], index="politics"),
          "politics-subjects": Strategy(["subject"], index="politics"),
          "politics-tfidf": Strategy(["TFIDF"], index="politics").prefix("title"),
          "politics-cfidf": Strategy(["CFIDF"], index="politics").prefix("title"),
          "politics-ctfidf": Strategy(["TFIDF", "CFIDF"], index="politics").prefix("title"),
          "politics-hfidf": Strategy([], ["HFIDF"], index="politics").prefix("title"),
          "politics-hcfidf": Strategy(["CFIDF"], ["HFIDF"], index="politics").prefix("title"),
          "politics-hctfidf": Strategy(["CFIDF", "TFIDF"], ["HFIDF"], index="politics").prefix("title"),
		  "politics-bm25": Strategy(["BM25"], index="politics").prefix("title"),
          "politics-bm25c": Strategy(["BM25C"], index="politics").prefix("title"),
          "politics-bm25ct": Strategy(["BM25", "BM25C"], index="politics").prefix("title"),
          
		  
          "politics-fulltext-f": Strategy(["fulltext"], index="politics"),
          "politics-subjects-f": Strategy(["subject"], index="politics"),
          "politics-tfidf-f": Strategy(["TFIDF"], index="politics").prefix("fulltext"),
          "politics-cfidf-f": Strategy(["CFIDF"], index="politics").prefix("fulltext"),
          "politics-ctfidf-f": Strategy(["TFIDF", "CFIDF"], index="politics").prefix("fulltext"),
          "politics-hfidf-f": Strategy([], ["HFIDF"], index="politics").prefix("fulltext"),
          "politics-hcfidf-f": Strategy(["CFIDF"], ["HFIDF"], index="politics").prefix("fulltext"),
          "politics-hctfidf-f": Strategy(["CFIDF", "TFIDF"], ["HFIDF"], index="politics").prefix("fulltext"),
          "politics-bm25-f": Strategy(["BM25"], index="politics").prefix("fulltext"),
		  "politics-bm25c-f": Strategy(["BM25C"], index="politics").prefix("fulltext"),
          "politics-bm25ct-f": Strategy(["BM25", "BM25C"], index="politics").prefix("fulltext"),
          
          
          "ntcir_subjects": Strategy(["subject"], index="ntcir_titles"),
          "ntcir_fulltext": Strategy(["fulltext"], index="ntcir_fulltext"),
          "ntcir_tfidf": Strategy(["TFIDF"], index="ntcir_titles").prefix("title"),
          "ntcir_cfidf": Strategy(["CFIDF"], index="ntcir_titles").prefix("title"),
          "ntcir_hfidf": Strategy([], ["HFIDF"], index="ntcir_titles").prefix("title"),
          "ntcir_hcfidf": Strategy(["CFIDF"], ["HFIDF"], index="ntcir_titles").prefix("title"),
          "ntcir_bm25": Strategy(["BM25"], index="ntcir_titles").prefix("title"),

        
          "ntcir_subjects_f": Strategy(["subject"], index="ntcir_fulltext"),
          "ntcir_tfidf_f": Strategy(["TFIDF"], index="ntcir_fulltext").prefix("fulltext"),
          "ntcir_cfidf_f": Strategy(["CFIDF"], index="ntcir_fulltext").prefix("fulltext"),
          "ntcir_hfidf_f": Strategy([], ["HFIDF"], index="ntcir_fulltext").prefix("fulltext"),
          "ntcir_hcfidf_f": Strategy(["CFIDF"], ["HFIDF"], index="ntcir_fulltext").prefix("fulltext"),
          "ntcir_bm25_f": Strategy(["BM25"], index="ntcir_fulltext").prefix("fulltext"),

          "trec_subjects": Strategy(["subject"], index="trec_titles"),
          "trec_fulltext": Strategy(["fulltext"], index="trec_fulltext"),
          "trec_tfidf": Strategy(["TFIDF"], index="trec_titles").prefix("title"),
          "trec_cfidf": Strategy(["CFIDF"], index="trec_titles").prefix("title"),
          "trec_hfidf": Strategy([], ["HFIDF"], index="trec_titles").prefix("title"),
          "trec_hcfidf": Strategy(["CFIDF"], ["HFIDF"], index="trec_titles").prefix("title"),
          "trec_bm25": Strategy(["BM25"], index="trec_titles").prefix("title"),
          
          "trec_subjects_f": Strategy(["subject"], index="trec_fulltext"),
          "trec_tfidf_f": Strategy(["TFIDF"], index="trec_fulltext").prefix("fulltext"),
          "trec_cfidf_f": Strategy(["CFIDF"], index="trec_fulltext").prefix("fulltext"),
          "trec_hfidf_f": Strategy([], ["HFIDF"], index="trec_fulltext").prefix("fulltext"),
          "trec_hcfidf_f": Strategy(["CFIDF"], ["HFIDF"], index="trec_fulltext").prefix("fulltext"),
          "trec_bm25_f": Strategy(["BM25"], index="trec_fulltext").prefix("fulltext"),
          # "ctfidf-nostem": Strategy(["TFIDF_nostem",
          #                            "CFIDF_nostem"]).prefix("title"),
          # "ctfidf-porter": Strategy(["TFIDF_porter",
          #                            "CFIDF_porter"]).prefix("title")
          # placeholder for the strategies which are not retrieved from Elastic but rather computed on the fly
          "mk": Strategy(["TFIDF"], index="economics").prefix("title"),
          "sm": Strategy(["TFIDF"], index="economics").prefix("title"),
          "letor": Strategy(["TFIDF"], index="economics").prefix("title")
          }

def execute_multisearch(index, strategy, queries, doc_type=None, prefix=None,
                        size=None):
    """TODO: Docstring for execute_multisearch.

    :index: TODO
    :doctype: TODO
    :prop: TODO
    :strategy: TODO
    :queries: TODO
    :returns: TODO

    """
    print('strategy: ', type(strategy), strategy._match)
    ms = MultiSearch(using=client, index=index, doc_type=doc_type) #index=strategy.get_index() , Tilman: ??
    for value in queries:
        s = Search().query(strategy.semantic_query(value))
        if size:
            s = s[0:size]
        ms = ms.add(s)
    responses = ms.execute()
    return responses

def execute_multi_singlesearch(index, strategy, queries, doc_type=None, prefix=None,
                                size=None, test_subject=False):
    """TODO: Docstring for execute_multisearch.

    :index: TODO
    :doctype: TODO
    :prop: TODO
    :strategy: TODO
    :queries: TODO
    :returns: TODO

    """
    for value in queries:
        if not test_subject:
          q = strategy.semantic_query(value)
        else:
          q = strategy.semantic_query_test(value)
        s = Search(using=client, index=index, doc_type=doc_type).query(q)
        print (q)
        if size:
            s = s[0:size]
        try:
            yield s.execute()
        except:
            print ("Warning: Skip executing this search")
            continue

def execute_singlesearch(index, strategy, query, doc_type=None, prefix=None,
                                size=None):
    """TODO: Docstring for execute_multisearch.

    :index: TODO
    :doctype: TODO
    :prop: TODO
    :strategy: TODO
    :queries: TODO
    :returns: TODO

    """
    q = strategy.semantic_query(query)
    s = Search(using=client, index=index, doc_type=doc_type).query(q)
    if size:
      s = s[0:size]
    return s.execute()

def perform_queries(index, doctype, prop, strategy, queries, size=500000,
                    source=False):
    """perform_queries
    :param index:
    the index to perform the queries to
    :param doctype:
    the document type to query for
    :param prop:
    the property to use, (such as 'title' or 'fulltext')
    :param strategy:
    the strategy to use (one of 'tfidf', 'bm25', 'cfidf', 'bm25c', 'ctfidf',
    'bm25ct')
    :param size:
    :param queries:
    :param source:
    Executes all the queries as a search query
    against index with doc_type and seperately against each field of fields
    returns a list of dicts
    """
    if "," in index:
        print("[elastify/utils.py] Warning: Unexpected behaviour may result\
        from selecting multiple indices", file=sys.stderr)

    try:
        strategy = strategy.prefix(prop)
    except AttributeError:
        strategy = FIELDS[strategy].prefix(prop)
    # fields = ['.'.join([prop, field]) for field in FIELDS[strategy]._match]

    # def querystring2query(querystring):
    #     """querystring2query
    #     :description: uses fields and size of outer namespace

    #     :param querystring: Transforms this querystring into an ES json query

    #     """
    #     query = {
    #         "_source": source,
    #         "query": {
    #             "multi_match": {
    #                 "fields": fields,  # tfidf,cfidf
    #                 "query": querystring.strip(),
    #                 "type": "most_fields"
    #             }
    #         },
    #         "size": size
    #     }
    #     return query

    # querysource.seek(0)  # reset readlines generator
    for querystring in queries:
        # multi_match should also be ok for only 1 field
        body = strategy.query_body(querystring)
        body["_source"] = source
        body["size"] = size
        # query = querystring2query(querystring)
        result = client.search(index=index, doc_type=doctype, body=body)
        docs = result['hits']['hits']
        # count = result['hits']['total']
        # print("total", count, "\t len docs", len(docs))
        yield docs
