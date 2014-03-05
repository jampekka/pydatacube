from collections import OrderedDict
import pydatacube

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
	data['value_dimensions'] = [
		dict(id='value', values=js_dataset['value'])
		]

	data['dimensions'] = []
	for dim_i, dim_id in enumerate(js_dimensions['id']):
		dimension = _load_dimension(js_dimensions, dim_i)
		data['dimensions'].append(dimension)
	return pydatacube._DataCube(data)

