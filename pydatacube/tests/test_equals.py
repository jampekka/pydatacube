from test_jsonstat import sample_cube
from testutils import *
import copy

def test_same_equals(sample_cube):
	assert sample_cube == sample_cube

def test_copy_equals(sample_cube):
	assert sample_cube == copy.deepcopy(sample_cube)

def test_empty_filter_equals(sample_cube):
	assert sample_cube == sample_cube.filter()

def test_filtered_not_equals(sample_cube):
	assert sample_cube != sample_filtering(sample_cube)

def test_same_filtering_equals(sample_cube):
	a = sample_filtering(sample_cube)
	b = sample_filtering(sample_cube.filter())
	assert a == b

def test_different_filtering_not_equals(sample_cube):
	assert sample_filtering(sample_cube, 1, 1) != sample_filtering(sample_cube, 0, 0)

def test_different_data_not_equals(sample_cube):
	other = copy.deepcopy(sample_cube)
	other._data['value_dimensions'][0]['values'][0] = not other._data['value_dimensions'][0]['values'][0]
	assert sample_cube != other

def test_different_metadata_not_equals(sample_cube):
	other = copy.deepcopy(sample_cube)
	other._data['metadata'] = "Hopefully no test object has this text in their label"
	assert sample_cube != other
