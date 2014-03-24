import copy
from test_jsonstat import sample_cube
from testutils import *

def test_dummy_filter(sample_cube):
	filtered = sample_cube.filter()
	assert sample_cube == filtered

def test_one_field_filter(sample_cube):
	spec = sample_cube.specification
	dimension = spec['dimensions'][1]
	category = dimension['categories'][1]
	filt = {dimension['id']: category['id']}
	filtered = sample_cube.filter(**filt)

	filtered_rows = map(dict, filtered.toEntries())
	all_entries = map(dict, sample_cube.toEntries())
	manual_filter = lambda row: row[dimension['id']] == category['id']
	manually_filtered = filter(manual_filter, all_entries)
	assert len(filtered_rows) == len(manually_filtered)
	for a, b in zip(filtered_rows, manually_filtered):
		assert a == b


def test_materialization(sample_cube):
	orig_items = map(list, sample_cube)
	orig_data = copy.deepcopy(sample_cube._data)

	filtered = sample_filtering(sample_cube)
	assert sample_cube != filtered
	
	materialized = filtered._materialize()
	assert filtered == materialized

	# Make sure the materialization doesn't affect
	# the original
	assert orig_data == sample_cube._data
	assert orig_items == map(list, sample_cube)
	
