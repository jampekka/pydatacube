import pytest
from pydatacube import jsonstat
from testutils import *

@pytest.fixture
def jsonstat_sample_dataset():
	import json
	return json.load(open('order.json'))['order']

@pytest.fixture
def sample_cube():
	dataset = jsonstat_sample_dataset()
	return jsonstat.to_cube(dataset)

def test_to_cube(jsonstat_sample_dataset):
	cube = jsonstat.to_cube(jsonstat_sample_dataset)

def test_to_jsonstat_and_back(sample_cube):
	cube = sample_cube
	# Make sure we end up with same cube after converting
	# the generated jsonstat back to a cube
	js = jsonstat.to_jsonstat_dataset(cube)
	cube2 = jsonstat.to_cube(js)
	assert cube._data == cube2._data


def test_filtered_to_jsonstat(sample_cube):
	spec = sample_cube.specification
	dimension = spec['dimensions'][0]
	category = dimension['categories'][0]
	filt = {dimension['id']: category['id']}
	filtered = sample_cube.filter(**filt)
	js = jsonstat.to_jsonstat(filtered)
	assert_data_equals(jsonstat.to_cube(js['dataset']), filtered)
	
