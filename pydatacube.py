import itertools
import copy
from collections import namedtuple

def cumprod(vals):
	cum = [vals[0]]
	for v in vals[1:]:
		cum.append(v*cum[-1])
	return cum

def dimension_magnitudes(sizes):
	return cumprod((sizes[1:]+[1])[::-1])[::-1]

class _Row(object):
	def __init__(self, cube, indices):
		self._cube = cube
		self._indices = indices
	
	def __iter__(self):
		for field_i, label_i in enumerate(self._indices):
			yield self._cube._category_label(field_i, label_i)
		
		yield self._cube._data['values'][self._cube._flatindex(self._indices)]
	

class _DataCube(object):
	def __init__(self, data, filters=None):
		self._data = data
		self._dim_sizes = [len(d['categories'])
			for d in data['dimensions']]
		self._dim_magnitudes = dimension_magnitudes(self._dim_sizes)
		self._dim_indices = {d['id']: i
			for (i, d) in enumerate(data['dimensions'])}
		self._cat_indices = {}
		for dim in data['dimensions']:
			self._cat_indices[dim['id']] = {c['id']: i
				for (i, c) in enumerate(dim['categories'])}

		if filters is None:
			self._filters = {}
		else:
			self._filters = filters

	def _dimension(self, idx):
		if not isinstance(idx, (int, long)):
			idx = self._dim_indices[idx]
		return self._data['dimensions'][idx]
	
	def _category_label(self, dimension, category_idx):
		dimension = self._dimension(dimension)
		category = dimension['categories'][category_idx]
		return category.get('label', category['id'])
			
	def _flatindex(self, indices):
		return sum(i*m for i, m in zip(indices, self._dim_magnitudes))
	
	def __iter__(self):
		dim_ranges = []
		for i in range(len(self._dim_sizes)):
			if i in self._filters:
				dim_ranges.append(self._filters[i])
			else:
				dim_ranges.append(xrange(self._dim_sizes[i]))

		for indices in itertools.product(*dim_ranges):
			yield _Row(self, indices)
	
	def dimensions(self):
		return [d['id'] for d in self._data['dimensions']]

	def filter(self, **kwargs):
		filters = copy.deepcopy(self._filters)
		for dim, categories in kwargs.items():
			if isinstance(categories, basestring):
				categories = [categories]
			categories = [self._cat_indices[dim][c] for c in categories]
			dim = self._dim_indices[dim]
			if dim not in self._filters:
				filters[dim] = []
			
			filters[dim].extend(categories)
		# TODO: Do this without recalculating stuff by implementing
		#	__new__ etc.
		return _DataCube(self._data, filters)
	
	def toTable(self):
		for row in self:
			yield list(row)

		# A bit faster with less nice API
		"""
		dim_ranges = map(xrange, self._dim_sizes)
		for i, indices in enumerate(itertools.product(*dim_ranges)):
			value_idx = self._flatindex(indices)
			labels = [self.category_label(field_i, label_i)
				for field_i, label_i in enumerate(indices)]
			yield labels + [self._data['values'][value_idx]]
		"""

