# encoding: utf-8
from collections import OrderedDict
import string
from pydatacube.pydatacube import _DataCube
import px_reader

# A bit scandinavian specific
default_translate = dict(zip(
	u"äöä -",
	u"aoa__"
	))

class Sluger(object):
	def __init__(self, translate=default_translate):
		self.given_out = {}
		self.translate = translate
	
	def __call__(self, value):
		slug = value.lower()
		chars = []
		for c in slug:
			c = self.translate.get(c, c)
			if c == '_':
				chars.append(c)
			if c.isalnum():
				chars.append(c)
		slug = ''.join(chars)
		slug = slug.encode('ascii', errors='ignore')
		realslug = slug
		# Hopefully won't happen
		while realslug in self.given_out:
			realslug = realslug + '_'
		return realslug

PxSyntaxError = px_reader.PxSyntaxError

def to_cube(pcaxis_data, Sluger=Sluger):
	px = px_reader.Px(pcaxis_data)
	cube = OrderedDict()
	metadata = OrderedDict()
	metadata['label'] = px.title
	cube['metadata'] = metadata
	
	if hasattr(px, 'codes'):
		codes = px.codes
	else:
		codes = {}

	dimensions = []
	dim_sluger = Sluger()
	for label, px_categories in px.values.iteritems():
		if label in px.codes:
			cat_ids = codes[label]
		else:
			cat_sluger = Sluger()
			cat_ids = [cat_sluger(c) for c in px_categories]
		
		categories = []
		for cat_id, cat_label in zip(cat_ids, px_categories):
			cat = dict(id=cat_id, label=cat_label)
			categories.append(cat)

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

