"""
Implementing various feature values for information retrieval from following sources:
- Chen, Ruey-Cheng, et al. "Harnessing semantics for answer sentence retrieval."
- 
"""
import collections

import math
import string
import sys
import json
import gensim
from collections import Counter

from nltk.corpus import stopwords as sw
from nltk.corpus import wordnet as wn
from nltk.stem.lancaster import LancasterStemmer
from nltk.stem.porter import PorterStemmer
from nltk.stem.snowball import EnglishStemmer

from elasticsearch import Elasticsearch

from .base import Features
from .feat_utils import *

class MKFeatures(Features):
	def __init__(self, client=Elasticsearch([{"host": "localhost"}], timeout=3600), ttfs={},
				 stemmer=StemmerType.PORTER_STEMMER, index='economics', index_field='title', doc_type='publication',
				 lm_index='economics', lm_index_field='fulltext', lm_doc_type='publication', mu=10.0):
		self._client = client
		self._index = index
		self._index_field = index_field
		self._doc_type = doc_type
		self._lm_index = lm_index
		self._lm_index_field = lm_index_field
		self._lm_doc_type = lm_doc_type
		self._stemmer = get_stemmer(stemmer)
		self._mu = mu
		self._stopwords_eng = set(sw.words('english'))
		self._ttfs = ttfs
		self._feature_map = {
			'sentence_length': self._calc_sentence_length,
			'exact_match': self._calc_exact_match,
			'term_overlap': self._calc_term_overlap,
			'synonym_overlap': self._calc_synonym_overlap,
			'language_model_score': self._calc_lms_dir
		}
		self._collection_length = get_field_stats(self._client, self._index, self._index_field, stats_fields=['sum_total_term_freq'])[ self._index_field]['sum_total_term_freq']

	def __str__(self):
		return 'MKFeatures'

	def calc_feature_values(self, query, sentence):
		result = {}
		for feature in self._feature_map.keys():
			func = self._feature_map[feature]
			result[feature] = func(query, sentence)
		return result


	""" Number of stems in the sentence """
	def _calc_sentence_length(self, query, sentence):
		return len(sentence.split())


	""" Whether query is a substring of the sentence """
	def _calc_exact_match(self, query, sentence):
		return int(query.lower() in sentence.lower())


	""" Fraction of query stems that occur in the sentence """
	def _calc_term_overlap(self, query, sentence):
		# stopping
		stopped_query = remove_stopwords(query, self._stopwords_eng, token_list=True)
		stopped_sentence = remove_stopwords(sentence, self._stopwords_eng, token_list=True)
		# stemming
		sentence_stems = [self._stemmer.stem(w).lower() for w in stopped_sentence]
		query_stems = [self._stemmer.stem(w).lower() for w in stopped_query]
		
		overlap = float(0)
		for q in query_stems:
			for s in sentence_stems:
				if q in s:
					overlap += 1
		#overlap = len([1 for stem in query_stems if stem in sentence_stems])
		ratio = float(overlap) / len(query_stems) if overlap > 0 else float(0)
		return ratio


	""" Fraction of query stems that occur or have a synonym in the sentence """
	def _calc_synonym_overlap(self, query, sentence):
		# stopping
		stopped_query = remove_stopwords(query, self._stopwords_eng, token_list=True)
		stopped_sentence = remove_stopwords(sentence, self._stopwords_eng, token_list=True)
		# stemming
		sentence_stems = [self._stemmer.stem(w).lower() for w in stopped_sentence]
		query_stems = [self._stemmer.stem(w).lower() for w in stopped_query]

		syn_list = []
		for term in query_stems:
			syns = wordnet_synonyms(term, True)
			for syn in syns:
				syn_list.append(self._stemmer.stem(syn))
		#print('syns for all q_terms: ', str(syn_list))
		overlap = float(0)
		for q in syn_list:
			for s in sentence_stems:
				if q in s:
					overlap += 1
		ratio = float(overlap) / len(syn_list) if overlap > 0 else float(0)
		return ratio


	""" Query likelihood of the sentence language model using Dirichlet smoothing """
	def _calc_lms_dir(self, query, sentence):
		#global ttfs
		query_stems = query.lower().split()

		sentence_tokens = sentence.lower().split()
		sentence_tf = collections.Counter(sentence_tokens)
		sentence_len = len(sentence_tokens)

		query_tf = collections.Counter(query_stems)
		score = float(0)
		for query_stem in query_stems:
			if query_stem in self._ttfs:
				cf = self._ttfs[query_stem]
			else:
				cf = get_term_stats(self._client, query_stem, self._lm_index, self._lm_doc_type, self._lm_index_field)
				self._ttfs[query_stem] = cf
			if cf == 0:
				continue
			#print('query_stem: ', query_stem)
			#print('sentence:', sentence)
			#print(sentence_tf[query_stem])
			score += float(query_tf[query_stem]) * math.log(float(sentence_tf[query_stem] + self._mu * float(cf) / self._collection_length)/ (sentence_len + self._mu))
		return score
