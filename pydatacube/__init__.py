"""Pydatacube - a library for handling statistical data tables

Pydatacube offers a simple API for handling statistical data
tables, especially those that are in a so called "cube format". Although
efficient in terms of computation and space, the cube format can be a bit
tedious to work with as is. Pydatacube simplifies working with such data
by exposing them over an API that feels like working with a two-dimensional
table (think CSV).

Most of the stuff is done in the _DataCube class, but to get data
in (and out), see converter modules pydatacube.jsonstat and
pydatacube.pcaxis.
"""

import copy
from abc import ABCMeta, abstractmethod, abstractproperty
import itertools, functools
from collections import Mapping

class lazy_dict(Mapping):
	def __init__(self, *args, **kwargs):
		self._store = dict(*args, **kwargs)
		self.lazy = {}
	
	def __getitem__(self, key):
		try:
			return self._store[key]
		except KeyError, e:
			val = self._store[key] = self.lazy.pop(key)(key)
			return val

		

	def __iter__(self):
		return itertools.chain(self._store, self.lazy)
	
	def __len__(self):
		return len(self._store) + len(self.lazy)

def method_memoize(func):
	cache = {}
	@functools.wraps(func)
	def cached(self, *args, **kwargs):
		try:
			object_cache = self.__memoize_cache
		except AttributeError:
			object_cache = self.__memoize_cache = {}

		try:
			cache = object_cache[func]
		except KeyError:
			cache = object_cache[func] = {}
		spec = (args, tuple(kwargs.items()))
		
		if spec not in cache:
			cache[spec] = func(self, *args, **kwargs)
		return cache[spec]
	return cached

class DataCubeBase(object):
	__metaclass__ = ABCMeta

	@abstractproperty
	def specification(self):
		return self._get_specification()
	
	@property
	def metadata(self):
		return self.specification['metadata']
	
	def _get_constant_dimensions(self):
		return (d for d in self.specification['dimensions']
			if 'categories' in d and len(d['categories']) == 1)
	
	@abstractmethod
	def filter(self, **kwargs): pass
	
	# TODO: Get rid of start and end by returning a sliceable
	# result
	@abstractmethod
	def rows(self, start=0, end=None, category_labels=False):
		pass
	
	def __iter__(self): return self.rows()
	
	@abstractmethod
	def __len__(self): pass
	

	# TODO: Get rid of start and end by returning a sliceable
	# result (although may get very weird)?
	def toColumns(self, start=0, end=None, dimension_labels=False,
			category_labels=False):
		# NOTE: This is here just for completeness. It is very slow
		# and should probably be avoided when possible
		get_label = lambda o: o.get('label', o['id'])
		get_id = lambda o: o['id']
		catval = dimval = get_id
		if category_labels:
			catval = get_label
		if dimension_labels:
			dimval = get_label
		
		dims = self.specification['dimensions']
		constant_dims = self._get_constant_dimensions()
		constant_vals = {d['id']: catval(d['categories'][0]) for d in constant_dims}
		
		results = []*len(dims)

		rows = self.rows(category_labels=category_labels)
		rows = itertools.islice(rows, start, end)
		cols = itertools.izip(*rows)

		result = []
		for dim, col in itertools.izip(dims, cols):
			if dim['id'] in constant_vals:
				result.append((dim, constant_vals[dim['id']]))
				continue
			result.append((dim, list(col)))
		result = {dimval(dim): col for dim, col in result}
		return result

	def group_by(self, *grouping_dims):
		dims = dict((d['id'], d) for d in self.specification['dimensions'])
		categories = (dims[d]['categories'] for d in grouping_dims)
		groupings = ((c['id'] for c in cat) for cat in categories)
		for subset in itertools.product(*list(groupings)):
			filt = dict(zip(grouping_dims, subset))
			yield self.filter(**filt)
	
	def group_for(self, *as_values):
		dim_ids = (d['id'] for d in self.specification['dimensions'])
		groups = set(dim_ids) - set(as_values)
		return self.group_by(*groups)

class UnionDataCube(DataCubeBase):
	def __init__(self, first, *others):
		self._first = first
		self._others = list(others)
		self._all = [self._first] + self._others
	
	@property
	@method_memoize
	def specification(self):
		dimhash = lambda spec: [(d['id'], d.get('label', d['id']))
			for d in spec['dimensions']]
		spec = self._first.specification
		spec = getattr(spec, '_store', spec)

		dims = dimhash(spec)
		for c in self._others:
			if dims != dimhash(spec):
				raise IncompatibleCubes("Can't union cubes with different dimensions")
			ospec = c.specification
			for dim, odim in zip(spec['dimensions'], ospec['dimensions']):
				# TODO: Having the categories (and dimensions) only as lists
				# is a pain (and probably somewhat slower)
				if 'categories' not in dim:
					continue

				for oc in odim['categories']:
					if oc['id'] in (c['id'] for c in dim['categories']):
						continue
					
					dim['categories'].append(oc)
				
				
		spec['metadata'] = {
			'label': '<union>',
			'specifications': [c.specification for c in self._all]
			}
		
		spec = lazy_dict(spec)
		spec.lazy['length'] = lambda: len(self)

		return spec
	
	def filter(self, **kwargs):
		return self.__class__(*(c.filter(**kwargs) for c in self._all))
	
	def rows(self, *args, **kwargs):
		for cube in self._all:
			# No yield from :(
			for row in cube.rows(*args, **kwargs):
				yield row
	

	
	@method_memoize
	def __len__(self):
		return sum(len(c) for c in self._all)

