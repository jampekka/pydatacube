# encoding: utf-8
from collections import OrderedDict
import string
from ..pydatacube import _DataCube
import px_reader

default_translate = string.maketrans(
	' -',
	'__')

class Sluger(object):
	def __init__(self, translate=default_translate):
		self.given_out = {}
		self.translate = translate
	
	def __call__(self, value):
		slug = value.encode('ascii', errors='ignore')
		slug = slug.lower()
		slug = slug.translate(self.translate)
		chars = []
		for c in slug:
			if c == '_':
				chars.append(c)
			if c.isalnum():
				chars.append(c)
		slug = ''.join(chars)
		realslug = slug
		# Hopefully won't happen
		while realslug in self.given_out:
			realslug = realslug + '_'
		return realslug

def to_cube(pcaxis_data, Sluger=Sluger):
	# TODO: Sluging of names for ids
	px = px_reader.Px(pcaxis_data)
	cube = OrderedDict()
	dimensions = []
	# Values is an ordered dict, so this
	# should go fine.
	dim_sluger = Sluger()
	for label, px_categories in px.values.iteritems():
		cat_sluger = Sluger()
		categories = [{'id': cat_sluger(c), 'label': c}
			for c in px_categories]
		dimension = dict(
			id=dim_sluger(label),
			label=label,
			categories=categories
			)
		dimensions.append(dimension)
	cube['dimensions'] = dimensions
	
	# TODO: Casting?
	# TODO: Add a public method to get raw
	#	data from a Px-object
	values = px._data.split()
	
	cube['value_dimensions'] = [
		dict(id=dim_sluger('value'), values=values)
		]

	return _DataCube(cube)

