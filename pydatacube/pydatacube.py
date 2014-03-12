import itertools
import copy
from collections import namedtuple, OrderedDict

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
		
		flat_i = self._cube._flatindex(self._indices)
		for value_dimension in self._cube._data['value_dimensions']:
			yield value_dimension['values'][flat_i]

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
	
	def materialize(self):
		if len(self._filters):
			raise NotImplemented("Materializing not yet implemented")
		return self
	
	@property
	def metadata(self):
		return self._data['metadata']

	def specification(self):
		spec = copy.copy(self._data)
		spec['length'] = len(self)
		enabled = self._enabled_dim_ranges()
		spec['dimensions'] = []
		for dim_i, origdim in enumerate(self._data['dimensions']):
			dim = copy.copy(origdim)
			origcats = dim['categories']
			dim['categories'] = []
			for cat_i in enabled[dim_i]:
				dim['categories'].append(
					origcats[cat_i])
			spec['dimensions'].append(dim)

		del spec['value_dimensions']
		for dim in self._data['value_dimensions']:
			novals = OrderedDict(
				(k, v) for (k, v) in dim.iteritems()
					if k != 'values'
				)
			spec['dimensions'].append(novals)
		return spec
	
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
		dim_ranges = self._enabled_dim_ranges()

		for indices in itertools.product(*dim_ranges):
			yield _Row(self, indices)
	
	def dimension_ids(self):
		ids = [d['id'] for d in self._data['dimensions']]
		ids += [d['id'] for d in self._data['value_dimensions']]
		return ids
	
	def _enabled_dim_ranges(self):
		dim_ranges = []
		for i in range(len(self._dim_sizes)):
			if i in self._filters:
				dim_ranges.append(self._filters[i])
			else:
				dim_ranges.append(range(self._dim_sizes[i]))
		return dim_ranges

	def filter(self, **kwargs):
		filters = copy.deepcopy(self._filters)
		for dim_id, categories in kwargs.items():
			if isinstance(categories, basestring):
				categories = [categories]
			categories = [self._cat_indices[dim_id][c] for c in categories]
			dim_i = self._dim_indices[dim_id]
			if dim_i not in self._filters:
				filters[dim_i] = []
			
			filters[dim_i].extend(categories)
		# TODO: Do this without recalculating stuff by implementing
		#	__new__ etc.
		return _DataCube(self._data, filters)
	
	def toTable(self):
		for row in self:
			yield list(row)
	
	def toEntries(self):
		ids = self.dimension_ids()
		for row in self:
			yield (itertools.izip(ids, row))
	
	def __len__(self):
		realsizes = [len(r) for r in self._enabled_dim_ranges()]
		mylen = 1
		for s in realsizes:
			mylen *= s
		return mylen

