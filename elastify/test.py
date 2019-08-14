from l2r_features.mk import MKFeatures
from functools import reduce
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, MultiSearch, Q
from elasticsearch_dsl.query import Bool, Query
from time import time
from l2r_features.mk import MKFeatures
from l2r_features.semantic import SemanticFeatures
from l2r_features.letor import LetorFeatures
try:
    import elastify.utils as utils
except ImportError:
    import utils

client = Elasticsearch([{"host": "localhost"}], timeout=3600)

def multisearch(index, strategy, queries, doc_type=None, prefix=None,size=None):
	print('strategy: ', type(strategy), strategy._match)
	ms = MultiSearch(using=client, index=index, doc_type=doc_type)
	response = []
	for value in queries:
		s = Search().query(strategy.semantic_query(value))
		if size:
			s = s[0:size]
		ms = ms.add(s)
	responses = ms.execute()
	return responses

def singlesearch(index, strategy, queries, doc_type=None, prefix=None,size=None):
	print('strategy: ', type(strategy), strategy._match)
	responses = []
	for value in queries:
		s = Search(using=client, index=index, doc_type=doc_type).query(strategy.semantic_query(value))
		if size:
			s = s[0:size]
		responses.append(s.execute())
	return responses



# def main():
# 	query = "hamburg city"
# 	sentence = "Metropolitan City under Transition: The Example of Hamburg in Germany"
# 	score = calc_language_model_score(query, sentence, 0.00001, index="economics", doc_type="publication", index_field="title")
# 	#field_stats = get_field_stats(index='economics', index_field='title', stats_fields=['sum_total_term_freq'])
# 	#term_stats = get_term_stats(index='economics', doc_type='publication', index_field='title', query=query)
# 	#print('field_stats: ', field_stats)
# 	#print('term_stats:', term_stats)
# 	print('lms: ', score)

# def main():
# 	index = 'economics'
# 	strategy = utils.FIELDS['tfidf']
# 	queries = ['financial crisis', 'chinese market'] * 200
# 	doc_type = 'publication'
# 	size = 10
# 	ts = time()
# 	q1 = multisearch(index, strategy, queries, doc_type=doc_type,size=size)
# 	print('multisearch took:', time() - ts)
# 	ts = time()
# 	q2 = singlesearch(index, strategy, queries, doc_type=doc_type,size=size)
# 	print('singlesearch took:', time() - ts)
# 	print(len(q1) == len(q2))

def main():
	qfile = open('/home/devnull/Development/python/masterproject/moving/Code/elastify/resources_tmp/queries-test.txt', 'r')
	model_bin_file = '/home/devnull/Development/python/masterproject/data/GoogleNews-vectors-negative300.bin'
	queries = [querystring.strip() for querystring in qfile.readlines()]
	total_term_freqs = {}
	mk = MKFeatures(client, ttfs=total_term_freqs)
	#sm = SemanticFeatures(model_bin_file)
	let = LetorFeatures(client, ttfs=total_term_freqs)
	for qid, docs in enumerate(utils.execute_multisearch('economics', utils.FIELDS['tfidf'], queries[:5], doc_type='publication', size=1)):
		for doc in docs:
			#print(doc.meta)
			print('sentence:', doc.title)
			print('query:', queries[qid])
			print('mk:', mk.calc_feature_values(queries[qid], doc.title))
			print()
	#		print('semantic:', sm.calc_feature_values(queries[qid], doc.title))
			print()
			print('letor:', let.calc_feature_values(queries[qid], doc.title))

if __name__ == "__main__": main()