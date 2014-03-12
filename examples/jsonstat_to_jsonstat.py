from pydatacube import jsonstat
import json
import urllib2
JSONSTAT_URL = "http://json-stat.org/samples/order.json"
DATASET = 'order'
data = json.load(urllib2.urlopen(JSONSTAT_URL))
dataset = data[DATASET]

# Convert to a pydatacube cube
cube = jsonstat.to_cube(dataset)
# Convert back to jsonstat
js = jsonstat.to_jsonstat(cube, dataset_name=DATASET)

# Verify that there's nothing fishy
js_again = jsonstat.to_jsonstat(jsonstat.to_cube(js[DATASET]), dataset_name=DATASET)
assert json.dumps(js) == json.dumps(js_again)

# And pretty print
print json.dumps(js, indent=4)
