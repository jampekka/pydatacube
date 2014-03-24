import itertools

def sample_filtering(cube, dim_i=1, cat_i=1):
	spec = cube.specification
	dimension = spec['dimensions'][dim_i]
	category = dimension['categories'][cat_i]
	filt = {dimension['id']: category['id']}
	filtered = cube.filter(**filt)
	return filtered
