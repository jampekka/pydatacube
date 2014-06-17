"""JSON-stat to pydatacube conversion"""

from collections import OrderedDict
import pydatacube

class JsonstatException(Exception): pass

def _load_dimension(dimensions, dim_i):
	dimension = OrderedDict()
	dimension['id'] = dimensions['id'][dim_i]
	jsonstat_dim = dimensions[dimension['id']]
	if 'label' in jsonstat_dim:
		dimension['label'] = jsonstat_dim['label']
	
	jsonstat_cats = jsonstat_dim['category']
	if 'index' in jsonstat_cats:
		category_ids = jsonstat_cats['index']
	else:
		category_ids = jsonstat_cats['label'].keys()
	
	if isinstance(category_ids, dict):
		items = [(v, k) for (k, v) in category_ids.items()]
		items.sort()
		category_ids = zip(*items)[1]
	
	categories = []
	
	try:
		labels = jsonstat_cats['label']
	except KeyError:
		labels = {}

	for cat_id in category_ids:
		category = OrderedDict()
		category['id'] = cat_id
		if cat_id in labels:
			category['label'] = labels[cat_id]
		categories.append(category)
		
	dimension['categories'] = categories

	return dimension

def to_cube(js_dataset):
	data = OrderedDict()
	js_dimensions = js_dataset['dimension']

	metadata = OrderedDict()
	if 'label' in js_dataset:
		metadata['label'] = js_dataset['label']
	data['metadata'] = metadata
	
	data['dimensions'] = []
	for dim_i, dim_id in enumerate(js_dimensions['id']):
		dimension = _load_dimension(js_dimensions, dim_i)
		data['dimensions'].append(dimension)
	
	data['value_dimensions'] = [
		dict(id='value', values=js_dataset['value'])
		]

	return pydatacube._DataCube(data)

class ConversionError(Exception): pass

def jsonstat_sanity_check(cube):
	value_dims = [d for d in cube.specification['dimensions'] if 'categories' not in d]
	if len(value_dims) != 1:
		raise ConversionError("Can produce jsonstat only from cubes with exactly one value dimension")

def can_convert(cube):
	try:
		jsonstat_sanity_check(cube)
		return True
	except ConversionError:
		return False

def _copyif(dst, src, key):
	if key not in src: return
	dst[key] = src[key]

def to_jsonstat_dataset(cube):
	jsonstat_sanity_check(cube)
	js = OrderedDict()
	ds = js['dataset'] = OrderedDict()
	_copyif(ds, cube.metadata, 'label')
	
	dims = ds['dimension'] = OrderedDict()
	vdims = []
	cdims = []

	for dim in cube.specification['dimensions']:
		if 'categories' in dim:
			cdims.append(dim)
		else:
			vdims.append(dim)
	dims['id'] = [d['id'] for d in cdims]
	dims['size'] = [len(d['categories']) for d in cdims]
	if len(vdims) != 1:
		raise JsonstatException("Can convert only datasets with exactly one value dimension")
	vdim = vdims[0]

	for cdim in cdims:
		dim = dims[cdim['id']] = OrderedDict()
		_copyif(dim, cdim, 'label')
		cats = dim['category'] = OrderedDict()
		ccats = cdim['categories']
		cats['index'] = [ccat['id'] for ccat in ccats]
		catlabels = OrderedDict()
		for ccat in ccats:
			if 'label' not in ccat:
				continue
			catlabels[ccat['id']] = ccat['label']
		if len(catlabels) > 0:
			cats['label'] = catlabels
		

	ds['value'] = cube._value_dimension_values()
	return ds

def to_jsonstat(cube, dataset_name='dataset'):
	js = OrderedDict()
	js[dataset_name] = to_jsonstat_dataset(cube)
	return js
