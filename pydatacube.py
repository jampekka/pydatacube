import itertools
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
	def __init__(self, data):
		self._data = data
		self._dim_sizes = [len(d['categories'])
			for d in data['dimensions']]
		self._dim_magnitudes = dimension_magnitudes(self._dim_sizes)
		self._dim_indices = {d['id']: i
			for (i, d) in enumerate(data['dimensions'])}

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
		dim_ranges = map(xrange, self._dim_sizes)
		for indices in itertools.product(*dim_ranges):
			yield _Row(self, indices)

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

