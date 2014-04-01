import pytest

from test_jsonstat import sample_cube

def test_group_for_vs_group_by(sample_cube):
	all_cols = sample_cube.dimension_ids()
	group_cols = all_cols[:2]
	value_cols = [c for c in all_cols if c not in group_cols]
	
	group_by = list(sample_cube.group_by(*group_cols))
	group_for = list(sample_cube.group_for(*value_cols))

	# Cubes don't (currently) implement __hash__, so
	# we can't use the nice set comparison
	assert group_by.__hash__ == None

	assert len(group_by) == len(group_for)
	for a in group_by:
		assert a in group_for

def test_no_value_grouping(sample_cube):
	value_cols = ['value']
	nonvalue_cols = sample_cube.dimension_ids()[:2]
	with pytest.raises(NotImplementedError):
		list(sample_cube.group_by(*value_cols))
	with pytest.raises(NotImplementedError):
		list(sample_cube.group_for(*nonvalue_cols))
