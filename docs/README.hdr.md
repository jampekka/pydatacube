# Pydatacube - a library for handling statistical data tables

Pydatacube offers a simple API for handling statistical data
tables, especially those that are in a so called "cube format". Although
efficient in terms of computation and space, the cube format can be a bit
tedious to work with as is. Pydatacube simplifies working with such data
by exposing them over an API that feels like working with a two-dimensional
table (think CSV).

Currently supports input and output of [JSON-stat](http://json-stat.org/)
and input of [PC-Axis](http://www.scb.se/sv_/PC-Axis/Start/) formats,
albeit supports only a subset of features of both.

Requires Python 2, most likely 2.7.

## Quickstart

```python
import json
import urllib2
from pydatacube import jsonstat

# Load jsonstat example data and pick a dataset
# in it
JSONSTAT_URL = "http://json-stat.org/samples/order.json"
DATASET = 'order'
data = json.load(urllib2.urlopen(JSONSTAT_URL))
dataset = data[DATASET]

# Convert to a pydatacube cube
cube = jsonstat.to_cube(dataset)
# Do some filtering
subcube = cube.filter(A=("1", "2"), C="4")
# And pretty printing
print cube.metadata['label']
for row in subcube:
	print("\t".join(map(str, row)))
```

# Usage

The following is also available as [Python code](examples/intro.py).

