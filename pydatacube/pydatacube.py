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
	
	def ids(self):
		for field_i, label_i in enumerate(self._indices):
			yield self._cube._category_id(field_i, label_i)
		
		flat_i = self._cube._flatindex(self._indices)
		for value_dimension in self._cube._data['value_dimensions']:
			yield value_dimension['values'][flat_i]
	
	def labels(self):
		for field_i, label_i in enumerate(self._indices):
			yield self._cube._category_label(field_i, label_i)
		
		flat_i = self._cube._flatindex(self._indices)
		for value_dimension in self._cube._data['value_dimensions']:
			yield value_dimension['values'][flat_i]

	def __iter__(self):
		return self.ids()

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
	
	__hash__ = None

	def __eq__(self, other):
		"""
		Tests if cubes are equal

		Cubes are equal when they represent exactly same data,
		including metadata and labels.

		NOTE: This can quite an expensive call, especially if
		the objects are identical!
		"""
		if self is other:
			return True
		
		if not isinstance(other, _DataCube):
			return False
		
		# Try first different tests to tell the
		# objects apart in about order of test
		# complexity.
		if len(self) != len(other):
			return False
		if self.metadata != other.metadata:
			return False
		if self.specification != other.specification:
			return False
		
		if self._data is other._data and self._filters == other._filters:
			return True

		# Finally go through all the values to
		# see if they are identical.
		for myval, otherval in itertools.izip(self, other):
			if list(myval) != list(otherval):
				return False
		return True
	
	def _materialize(self):
		"""
		Make a "standalone" version of a filtered datacube
		
		Should be needed only internally by stuff that accesses
		_data.

		NOTE: Doesn't fully copy the data, ie changes to the
		materialized still reflect to the original. Keep in mind
		that we should be considered immutable.
		"""
		if len(self._filters) == 0:
			return self
		
		data = copy.copy(self._data)
		enabled_dim_idx = self._enabled_dim_ranges()
		data['dimensions'] = copy.copy(data['dimensions'])
		for dim_i, cat_idx in enumerate(enabled_dim_idx):
			dim = data['dimensions'][dim_i] = copy.copy(data['dimensions'][dim_i])
			dim['categories'] = [dim['categories'][i] for i in cat_idx]
		
		valdims = data['value_dimensions'] = copy.copy(data['value_dimensions'])
		validx = map(self._flatindex, itertools.product(*self._enabled_dim_ranges()))
		for i in range(len(valdims)):
			valdim = valdims[i] = copy.copy(valdims[i])
			values = valdim['values']
			valdim['values'] = [values[i] for i in validx]

		return _DataCube(data)
	
	@property
	def metadata(self):
		return self._data['metadata']
	
	@property
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
	
	def _category_id(self, dimension, category_idx):
		dimension = self._dimension(dimension)
		return dimension['categories'][category_idx]['id']
			
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
	
	def dimension_labels(self):
		label = lambda d: d.get('label', d['id'])
		
		ids = [label(d) for d in self._data['dimensions']]
		ids += [label(d) for d in self._data['value_dimensions']]
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
				filters[dim_i] = set()
			
			filters[dim_i] = set(categories)
		# TODO: Do this without recalculating stuff by implementing
		#	__new__ etc.
		return _DataCube(self._data, filters)
	
	def toTable(self, labels=False):
		if labels:
			rowiter = lambda row: row.labels()
		else:
			rowiter = lambda row: row.ids()
		for row in self:
			yield list(rowiter(row))
	
	def toEntries(self, dimension_labels=False, category_labels=False):
		if dimension_labels:
			dims = self.dimension_labels()
		else:
			dims = self.dimension_ids()

		if category_labels:
			rowiter = lambda row: row.labels()
		else:
			rowiter = lambda row: row.ids()
		
		for row in self:
			yield (itertools.izip(dims, rowiter(row)))
	
	def toColumns(self,
			start=0, end=None,
			dimension_labels=False, category_labels=False,
			collapse_unique=True):
		# TODO: Return iterators instead of the start-end hassle?
		if dimension_labels:
			dims = self.dimension_labels()
		else:
			dims = self.dimension_ids()

		if category_labels:
			rowiter = lambda row: row.labels()
		else:
			rowiter = lambda row: row.ids()

		dataset = itertools.islice(iter(self), start, end)

		rows = (rowiter(row) for row in dataset)
		cols = list(itertools.izip(*rows))
		if collapse_unique:
			# TODO: Don't fetch them in the first place!
			for i, rng in enumerate(self._enabled_dim_ranges()):
				if len(rng) == 1:
					cols[i] = cols[i][0]

		return OrderedDict(zip(dims, list(cols)))
		
	def group_for(self, *as_values):
		groups = set(self.dimension_ids()) - set(as_values)
		return self.group_by(*groups)
	
	def group_by(self, *grouping_columns):
		value_cols = [d['id'] for d in self._data['value_dimensions']]

		value_groups = set(value_cols) & set(grouping_columns)
		if len(value_groups) > 0:
			raise NotImplementedError("Grouping by value columns (%s) not implemented"%(value_groups,))
		group_idx = [self._dim_indices[id] for id in grouping_columns]
		dim_idx, groupings = zip(*[(i, r) for
			(i, r) in enumerate(self._enabled_dim_ranges())
			if i in group_idx])
		dimension_ids = [dim['id'] for dim in self._data['dimensions']]

		for subset in itertools.product(*groupings):
			filt = {}
			for i, cat_i in enumerate(subset):
				dim_i = dim_idx[i]
				dim_id = dimension_ids[dim_i]
				cat_id = self._category_id(dim_i, cat_i)
				filt[dim_id] = cat_id
			yield self.filter(**filt)

	
	def __len__(self):
		realsizes = [len(r) for r in self._enabled_dim_ranges()]
		mylen = 1
		for s in realsizes:
			mylen *= s
		return mylen

