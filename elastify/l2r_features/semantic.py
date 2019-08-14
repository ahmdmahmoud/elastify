"""
Implementing various feature values for information retrieval from following sources:
- Chen, Ruey-Cheng, et al. "Harnessing semantics for answer sentence retrieval."
- 
"""
import sys
import gensim
from .base import Features

class SemanticFeatures(Features):
	def __init__(self, w2v_modelfile=None):
		self._feature_map = {
			'word2vec': self._calc_word2vec,
		#    'esa_cos_sim': self._calc_esa_cos_sim
		}
		try:
			print('loading pre-trained word2vec model, this may take a while...')
			self.w2v_model = gensim.models.Word2Vec.load_word2vec_format(w2v_modelfile, binary=True)
			self.w2v_model.init_sims(replace=True)
			print('finished')
		except:
			print('No valid word2vec model provided')
			sys.exit(0)

	def __str__(self):
		return 'SemanticFeatures'

	def calc_feature_values(self, query, sentence):
		result = {}
		for feature in self._feature_map.keys():
			func = self._feature_map[feature]
			result[feature] = func(query, sentence)
		return result

	def _calc_word2vec(self, query, sentence):

		query_terms = [t.lower() for t in query.split() if t in self.w2v_model]
		sentence_terms = [t.lower() for t in sentence.split() if t in self.w2v_model]
		#print('query_terms', query_terms)
		#print('sentence_terms', sentence_terms)
		score = 0.0
		if len(sentence_terms) > 0 and len(query_terms) > 0:
			try:
				score = self.w2v_model.n_similarity(query_terms, sentence_terms)
			except KeyError:
				score = 0.0
		return score
