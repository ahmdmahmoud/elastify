import abc

class Features(metaclass=abc.ABCMeta):
	def __init__(self, func_map={}):
		self._feature_map = func_map

	@property
	def func_map(self):
		return self._feature_map

	@abc.abstractmethod
	def calc_feature_values(self, query, sentence):
		"""
		Calculate the feature values and
		return a dict with the feature names as keys
		and the corresponding scores as values
		"""
		pass

	def get_feature_names(self):
		"""
		Return the names of the every single feature 
		calculated by this feature class
		"""
		return list(self._feature_map.keys())