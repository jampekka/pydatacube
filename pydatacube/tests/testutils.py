import itertools

def assert_data_differs(cube1, cube2):
	if len(cube2) != len(cube1):
		return
	
	for row1, row2 in itertools.izip(cube1, cube2):
		if list(row1) != list(row2):
			return
	assert True

def assert_data_equals(cube1, cube2):
	assert len(cube1) == len(cube2)
	for row1, row2 in itertools.izip(cube1, cube2):
		assert list(row1) == list(row2)

def sample_filtering(cube):
	spec = cube.specification
	dimension = spec['dimensions'][1]
	category = dimension['categories'][1]
	filt = {dimension['id']: category['id']}
	filtered = cube.filter(**filt)
	return filtered
