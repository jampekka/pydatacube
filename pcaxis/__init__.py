from ..pydatacube import _DataCube
import px_reader

def to_cube(pcaxis_data):
	# TODO: Sluging of names for ids
	px = px_reader.Px(pcaxis_data)
	cube = {}
	dimensions = []
	# Values is an ordered dict, so this
	# should go fine.
	for label, px_categories in px.values.iteritems():
		categories = [{'id': c} for c in px_categories]
		dimension = dict(
			id=label,
			categories=categories
			)
		dimensions.append(dimension)
	cube['dimensions'] = dimensions
	# TODO: Casting?
	# TODO: Add a public method to get raw
	#	data from a Px-object
	cube['values'] = px._data.split()
	return _DataCube(cube)

