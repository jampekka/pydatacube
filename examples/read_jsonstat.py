import json
import urllib2

#from pydatacube import from_jsonstat_dimension
from pydatacube import jsonstat

#JSONSTAT_URL = "http://json-stat.org/samples/oecd-canada.json"
#DATASET = 'oecd'
JSONSTAT_URL = "http://json-stat.org/samples/order.json"
DATASET = 'order'

data = json.load(urllib2.urlopen(JSONSTAT_URL))
dataset = data[DATASET]
cube = jsonstat.to_cube(dataset)

#from pprint import pprint
#pprint(cube)

def pprint_table(table):
	for row in table:
		print "\t".join(map(str, row))

table = list(cube.toTable())
pprint_table(table)
