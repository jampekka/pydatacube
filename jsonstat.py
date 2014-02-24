import pydatacube

def _load_dimension(dimensions, dim_i):
	dimension = {}
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
		category = {}
		category['id'] = cat_id
		if cat_id in labels:
			category['label'] = labels[cat_id]
		categories.append(category)
		
	dimension['categories'] = categories

	return dimension

def to_cube(js_dataset):
	data = {}
	data['values'] = js_dataset['value']
	js_dimensions = js_dataset['dimension']
	data['dimensions'] = []
	for dim_i, dim_id in enumerate(js_dimensions['id']):
		dimension = _load_dimension(js_dimensions, dim_i)
		data['dimensions'].append(dimension)
	return pydatacube._DataCube(data)

