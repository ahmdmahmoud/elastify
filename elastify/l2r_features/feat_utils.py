import itertools
from nltk.corpus import stopwords as sw
from nltk.corpus import wordnet as wn

from nltk.stem.lancaster import LancasterStemmer
from nltk.stem.porter import PorterStemmer
from nltk.stem.snowball import EnglishStemmer

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from enum import IntEnum

""" Return WordNet synonyms """
def wordnet_synonyms(term, include_term=False):
    names = [synset.lemma_names() for synset in wn.synsets(term)]
    if include_term:
        names.append([term])
    return sorted(unique(itertools.chain(*names)))


""" Remove stopwords from text """
def remove_stopwords(text, stopwords, token_list=False, concat_char=' '):
    cleaned = [i for i in text.lower().split() if i not in stopwords]
    if token_list:
        return cleaned
    else:
        concat_text(cleaned, concat_char)


""" Get tf and df frequencies for query stems from index, doc_type and fields"""
def get_freq(client, query_stems, index, doc_type, field):
    result = {}
    for query_stem in query_stems:

        s = Search(using=client, index=index, doc_type=doc_type)
        s.update_from_dict({
                                "explain": "true",
                                "query": {
                                    "constant_score": {
                                        "filter": {
                                            "term" : { field : query_stem }
                                        }
                                    }
                                }
                            })
        resp = s.execute()
        #global_hits = [hit.meta.explanation.details[0].details[0].details[0].details[0].value for hit in resp.hits]
        result.update({
            query_stem: resp.hits.total # could also be cf
        })
    return result

def get_doc_count(client, index='economics'):
    s = Search(using=client, index=index)
    s.params(search_type="count")
    s.update_from_dict({
        "aggs": {
            "count_by_type": {
                "terms": {
                    "field": "_type"
                }
            }
        }
    })
    resp = s.execute()
    return resp.hits.total


""" Field Statistics: returns the given statistical field values of the index and field """
def get_field_stats(client, index, index_field, stats_fields):
    resp = client.field_stats(index=index, fields=[index_field], level='indices')
    result = {}
    s_stats = {}
    for s_field in stats_fields:
        s_stats.update({
            s_field: resp['indices'][index]['fields'][index_field][s_field]
        })
    result.update({
        index_field: s_stats
    })
    return result

""" Term Statistics: TODO """
def get_term_stats(client, query_stem, index, doc_type, index_field):
    """ we have to artificially create the doc body for the query stem to retrieve term statistics """
    resp = client.termvectors(index=index, doc_type=doc_type, body={'doc': {index_field: query_stem}}, fields=[index_field], offsets=False, payloads=False,
                                positions=False, realtime=True, field_statistics=False, term_statistics=True)
    # if the term doesn't occur at all, not zero is returned but no key at all
    #print (list(resp['term_vectors'][index_field]['terms'].keys())[0])
    try: 
        key = list(resp['term_vectors'][index_field]['terms'].keys())[0]
    except KeyError:
        return 0
    if 'ttf' in resp['term_vectors'][index_field]['terms'][key]:
        return resp['term_vectors'][index_field]['terms'][key]['ttf']
    else:
        return 0

def unique(seq):
    """ Return unique items in seq. """
    seen = set()
    return [x for x in seq if x not in seen and not seen.add(x)]

class StemmerType(IntEnum):
    """
    @brief Enumeration of NLTK stemmers that can be used
    """
    PORTER_STEMMER = 0
    LANCASTER_STEMMER = 1
    ENGLISH_STEMMER = 2

"""get the stemmer, default is PorterStemmer"""
def get_stemmer(stemmer_type):
    stemmers = {
        0: PorterStemmer(),
        1: LancasterStemmer(),
        2: EnglishStemmer()
    }
    return stemmers.get(stemmer_type, PorterStemmer())

