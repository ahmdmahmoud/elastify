from nltk.corpus import stopwords as sw
from .feat_utils import get_stemmer, StemmerType, get_freq, get_doc_count, get_field_stats, get_term_stats, remove_stopwords
from .base import Features
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from collections import Counter
import math

class LetorFeatures(Features):

	def __init__(self, client=Elasticsearch([{"host": "localhost"}], timeout=3600), ttfs={},
		stemmer=StemmerType.PORTER_STEMMER, index='economics', index_field='title', doc_type='publication',
		lm_index='economics', lm_index_field='fulltext', lm_doc_type='publication', delta=0.7, mu=2000.0, alpha=0.1):
		
		self._client = client
		self._index = index
		self._index_field = index_field
		self._lm_index = lm_index
		self._lm_index_field = lm_index_field
		self._lm_doc_type = lm_doc_type
		self._doc_type = doc_type
		self._stemmer = get_stemmer(stemmer)
		self._stopwords_eng = set(sw.words('english'))
		self._ttfs = ttfs
		self._idfs = {}
		self._delta = delta
		self._alpha = alpha
		self._mu = mu
		self._feature_map = {
			"cov_query_term_nr": self._cov_query_term_nr,
			"idf_title": self._idf_title,
			"sum_tf": self._sum_tf,
			"min_tf": self._min_tf,
			"max_tf": self._max_tf,
			"mean_tf": self._mean_tf,
			"var_tf": self._var_tf,
			"sum_length_norm_tf": self._sum_length_norm_tf,
			"min_length_norm_tf": self._min_length_norm_tf,
			"max_length_norm_tf": self._max_length_norm_tf,
			"mean_length_norm_tf": self._mean_length_norm_tf,
			"var_length_norm_tf": self._var_length_norm_tf,
			"sum_tfidf": self._sum_tfidf,
			"min_tfidf": self._min_tfidf,
			"max_tfidf": self._max_tfidf,
			"mean_tfidf": self._mean_tfidf,
			"var_tfidf": self._var_tfidf,
			"lms_abs": self._lms_abs,
			"lms_ds": self._lms_ds,
			"lms_jm": self._lms_jm
		}
		self._collection_length = get_field_stats(self._client, self._index, self._index_field, stats_fields=['sum_total_term_freq'])[ self._index_field]['sum_total_term_freq']

	def __str__(self):
        	return 'LetorFeatures'
		
	def calc_feature_values(self, query, sentence):

		self._tfidf_dict = {}
		self._idf_dict = {}

		# calculating term frequencies
		tfs = {}
		for t in query.lower().split():
			tfs[t] = sentence.lower().count(t)
		m = max(tfs.values())

		if m > 0:
			for tf in tfs.keys():
				tfs[tf] = tfs[tf] / m
		self._tf = tfs
		
		for q in query.lower().split():
			if q in self._idfs:
				idf = self._idfs[q]
			else:
				s = Search(using=self._client, index=self._index, doc_type=self._doc_type).query(Q('multi_match', query=q, fields=[self._index_field]))
				idf = math.log( (get_doc_count(client=self._client,index=self._index) - s.count() + 0.5) / (s.count() + 0.5), 10)
				self._idfs[q] = idf
			self._idf_dict[q] = idf
			
			self._tfidf_dict[q] = self._tf[q] * idf

		length = len(tfs.values())
		self._norm_tfs = [i / length for i in self._tf.values()]

		result = {}
		for feature in self.func_map.keys():
			func = self.func_map[feature]
			result[feature] = func(query, sentence)
		return result

	# covered query term number, nearly like overlap but doesn't divide by query term length
	def _cov_query_term_nr(self, query, sentence):
		stopped_query = remove_stopwords(query, self._stopwords_eng, token_list=True)
		stopped_sentence = remove_stopwords(sentence, self._stopwords_eng, token_list=True)
		sentence_stems = [self._stemmer.stem(w).lower() for w in stopped_sentence]
		query_stems = [self._stemmer.stem(w).lower() for w in stopped_query]
		return sum(map(sentence_stems.count, query_stems))

	# idf 
	def _idf_title(self, query, sentence):
		#### id 18 = idf title
		return sum(self._idf_dict.values())

	# sum of tf
	def _sum_tf(self, query, sentence):
		return sum(self._tf.values())

	# minimum of tf
	def _min_tf(self, query, sentence):
		return min(self._tf.values())

	# maximum of tf
	def _max_tf(self, query, sentence):
		return max(self._tf.values())

	# mean of tf
	def _mean_tf(self, query, sentence):
		vals = self._tf.values()
		mean = sum(vals)/len(vals)
		return mean

	# variance of tf
	def _var_tf(self, query, sentence):
		tfs_values = self._tf.values()
		mean = self._mean_tf(query, sentence)
		variance = sum([(i-mean)**2 for i in tfs_values]) / len(tfs_values)
		return variance

	# sum of length normalized tf
	def _sum_length_norm_tf(self, query, sentence):
		return sum(self._norm_tfs)

	# minimum of length normalized tf
	def _min_length_norm_tf(self, query, sentence):
		return min(self._norm_tfs)

	# maximum of length normalized tf
	def _max_length_norm_tf(self, query, sentence):
		return max(self._norm_tfs)

	# mean of length normalized tf
	def _mean_length_norm_tf(self, query, sentence):
		return sum(self._norm_tfs)/len(self._norm_tfs)

	# variance of length normalized tf
	def _var_length_norm_tf(self, query, sentence):
		norm_mean = self._mean_length_norm_tf(query, sentence)
		norm_variance = sum([(i-norm_mean)**2 for i in self._norm_tfs]) / len(self._norm_tfs)
		return norm_variance

	# sum of tfidf
	def _sum_tfidf(self, query, sentence):
		return sum(self._tfidf_dict.values())

	# minimum of tfidf
	def _min_tfidf(self, query, sentence):
		return min(self._tfidf_dict.values())

	# maximum of tfidf
	def _max_tfidf(self, query, sentence):
		return max(self._tfidf_dict.values())

	# mean of tfidf
	def _mean_tfidf(self, query, sentence):
		tfidf_mean = sum(self._tfidf_dict.values())/len(self._tfidf_dict.values())
		return tfidf_mean

	# variance of tf-idf
	def _var_tfidf(self, query, sentence):
		tfidf_variance = sum([(i-self._mean_tfidf(query, sentence))**2 for i in self._tfidf_dict.values()]) / len(self._tfidf_dict.values())
		return tfidf_variance

	# language model with absolute discounting smoothing
	def _lms_abs(self, query, sentence):
		query_stems =  query.lower().split()
		sentence_tokens = sentence.lower().split()
		sentence_tf = Counter(sentence_tokens)
		sentence_len = len(sentence_tokens)

		score = float(0)
		for query_stem in query_stems:
			if query_stem in self._ttfs:
				cf = self._ttfs[query_stem]
			else:
				cf = get_term_stats(self._client, query_stem, self.self._lm_index,self._doc_type, self._lm_index_field)
				self._ttfs[query_stem] = cf
			if cf == 0:
				continue

			score += math.log(1 + (max(sentence_tf[query_stem] - self._delta, 0) / sentence_len) + (self._delta * len(sentence_tf) / sentence_len)*(float(cf) / self._collection_length))
		return score

	# language model with bayesian smoothing using dirichlet priors
	def _lms_ds(self, query, sentence):
		query_stems =  query.lower().split()
		sentence_tokens = sentence.lower().split()
		sentence_tf = Counter(sentence_tokens)
		sentence_len = len(sentence_tokens)

		score = float(0)
		for query_stem in query_stems:
			if query_stem in self._ttfs:
				cf = self._ttfs[query_stem]
			else:
				cf = get_term_stats(self._client, query_stem, self.self._lm_index,self._doc_type, self._lm_index_field)
				self._ttfs[query_stem] = cf
			if cf == 0:
				continue
			score += math.log(1 + (sentence_tf[query_stem] + self._mu * (float(cf) / self._collection_length)) / (sentence_len + self._mu))
		return score

	# language model with jelinek-mercer smoothing
	def _lms_jm(self, query, sentence):
		#we can try stemming here to improve?
		query_stems =  query.lower().split()
		sentence_tokens = sentence.lower().split()
		sentence_tf = Counter(sentence_tokens)
		sentence_len = len(sentence_tokens)
		
		score = float(0)
		for query_stem in query_stems:
			if query_stem in self._ttfs:
				cf = self._ttfs[query_stem]
			else:
				cf = get_term_stats(self._client, query_stem, self.self._lm_index,self._doc_type, self._lm_index_field)
				self._ttfs[query_stem] = cf
			if cf == 0:
				continue
			max_lik = sentence_tf[query_stem] / sentence_len
			score += math.log( 1 + (1.0-self._alpha) * max_lik + self._alpha * (float(cf) / self._collection_length))
		return score
