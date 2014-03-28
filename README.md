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

## Imports and setup

To get a data in, it has to be in a format readable by importers. The internal data format is in a bit of a flux, so input and output should be only in these formats, meaning currently JSON-stat. We use JSON-stat in this example, but similar stuff for PC-Axis can be done using the `pydatacube.pcaxis`-module.

```python
import pydatacube.jsonstat
```


The JSON-stat importer doesn't actually read json itself, but it's Python-representation, so we'll need the `json`-module to import it.

```python
import json
```


And the sample data is fetched from the web, so we'll need `urllib2`.

```python
import urllib2
```


The API makes heavy usage of iterators, so we'll use the islice-function to print only parts of data in the examples below. The chain is used to "join" iterators, eg `list(chain(['a'], ['b']))` is equivalent to `['a']+['b']`.

```python
from itertools import islice, chain
```
And who doesn't like prettier prints at times.

```python
from pprint import pprint
```


## Getting data in

We'll download a sample file from JSON-stat website and convert it to Python-representation.

```python
jsonstat_raw_data = urllib2.urlopen(
	"http://json-stat.org/samples/oecd-canada.json")
jsonstat_data = json.load(jsonstat_raw_data)
```


JSON-stat has the notion of "datasets" that allows multiple datasets be embedded in one file, although these are otherwise mostly independent. Pydatacube represents each cube separately, so we'll have to select a dataset. The datasets are simply dictionary items in the top-level of the JSON-stat structure. In here we study the oecd-dataset.

```python
jsonstat_dataset = jsonstat_data["oecd"]
```


The dataset can then be converted to a `pydatacube.pydatacube._DataCube`-object using the converter. Note the underscore (and the tedious module name) in the class name which indicates here that the `_DataCube`'s constructor is private.  Of course it's possible to create a cube using the internal data format, but this is likely to break in the future.

```python
cube = pydatacube.jsonstat.to_cube(jsonstat_dataset)
```


## Reading the data

Cube-objects aim to represent the data as if it's just a flat two-dimensional table. An example of this is iterating the cube object, which gives (iterators to) the rows. Taking data out of the cube object often an iterator. This gets quite handy as the tables can be very large.

```python
for row in islice(cube, 5):
	# The rows themselves are also iterators,
	# so they'll have to be converted
	# to lists for nicer viewing.
	print(list(row))
```
```
[u'UNR', u'AU', u'2003', 5.943826289]
[u'UNR', u'AU', u'2004', 5.39663128]
[u'UNR', u'AU', u'2005', 5.044790587]
[u'UNR', u'AU', u'2006', 4.789362794]
[u'UNR', u'AU', u'2007', 4.379649386]
```


As the printout shows, we get the rows as lists.

### ids, labels and values

However, this printout is missing the column ids, which all cubes always have. The columns are actually named *dimensions*, following JSON-stat. They can be digged from the cube's *specification*, which describes all sorts of stuff of the object. The specification is just a dict nested with other dicts and lists. This can be a bit tedious and may be subject to change. But anyway, let's get the specification,

```python
specification = cube.specification
```
and dig out the dimension ids and print them out.

```python
dimension_ids = [dimension['id'] for dimension in specification['dimensions']]
print(dimension_ids)
```
```
[u'concept', u'area', u'year', 'value']
```


Combining these, we have a quick and dirty CSV-exporter:

```python
print(",".join(dimension_ids))
for row in islice(cube, 5):
	row_as_strings = map(str, row)
	print(",".join(row_as_strings))
```
```
concept,area,year,value
UNR,AU,2003,5.943826289
UNR,AU,2004,5.39663128
UNR,AU,2005,5.044790587
UNR,AU,2006,4.789362794
UNR,AU,2007,4.379649386
```


The columns (=dimensions) and "certain" values (=categories) usually have human-readable labels. The category and "non-category" values have a deeper meaning in the cube data structure, but usually this can be ignored. The main difference is that categories are always strings and have ids and usually labels, whether the "non-category" values are usually numbers. For these "non-category" values requesting either id or label results just to the number. And even for categories, the id is used as label if no label is specified.

So we can quite easily do a quick and dirty CSV-exporter that uses the labels. We'll have to use semicolons separator as the area-column name uses commas.

```python
dimension_labels = [dimension.get('label', dimension['id'])
	for dimension in specification['dimensions']]
print(";".join(dimension_labels))
for row in islice(cube, 5):
	row_labels_as_strings = map(str, row.labels())
	print(";".join(row_labels_as_strings))
```
```
Selected indicator;OECD countries, EU15 and total;2003-2014;value
Unemployment rate;Australia;2003;5.943826289
Unemployment rate;Australia;2004;5.39663128
Unemployment rate;Australia;2005;5.044790587
Unemployment rate;Australia;2006;4.789362794
Unemployment rate;Australia;2007;4.379649386
```


There are a few other "formats" in which we can get the data out. `_DataCube.toEntries` spits the rows out as dicts and `_DataCube.toColumns` as dict of column value lists. The latter get's quite handy in for example plotting where the columns are usually needed as lists.

```python
columns = cube.toColumns(end=3)
#Convert from OrderedDict to dict for prettier printing
columns = dict(columns)
pprint(columns)
```
```
{u'area': (u'AU', u'AU', u'AU'),
 u'concept': u'UNR',
 'value': (5.943826289, 5.39663128, 5.044790587),
 u'year': (u'2003', u'2004', u'2005')}
```


Note how 'concept'-column in the printout gets only one value. The column output by default "collapses" like that if it has the same value for every row in the dataset.  This becomes quite useful when working with filtering.

## Filtering

Usually the datasets include quite a bit of data and we're interested in one subset at a time. For this, there's a method `_DataCube.filter`, which allows picking just rows that we are interested in. Note that filtering itself is actually a really lightweight operation, it just creates a new cube with the specified filters, which are then used whenever we want data out.

For example, we can get the unemployment statistics for Finland by filtering by the area and concept-columns. The `filter`-method produces yet another cube, but only with stuff we want. We select just two years for a reasonable printout here.

```python
finnish_unemployment_lately = cube.filter(
	area='FI',
	concept='UNR',
	year=('2013', '2014'))
pprint(dict(finnish_unemployment_lately.toColumns()))
```
```
{u'area': u'FI',
 u'concept': u'UNR',
 'value': (7.962718148, 7.757742455),
 u'year': (u'2013', u'2014')}
```


As the result of filtering is also a `_DataCube`, it can be further filtered by the very same `filter`-method.

```python
finnish_unemployment_2014 = finnish_unemployment_lately.filter(
	year='2014')
pprint(dict(finnish_unemployment_2014.toColumns()))
```
```
{u'area': u'FI', u'concept': u'UNR', 'value': (7.757742455,), u'year': u'2014'}
```


## Grouping

Another main feature for working with the data is "grouping", done by `_DataCube.groups`. This need often arises when we want to compare some statistics among different groups by eg. plotting. For example, let's compare how unemployment has progressed in Finland and Sweden over the years. We'll start by first filtering for only stuff we want.

```python
fi_and_swe_unemployment = cube.filter(
	area=('FI', 'SE'),
	concept='UNR')
```


From the filtered cube we can get one cube for each country by grouping. The `_DataCube.groups` method, a bit unintuitively perhaps, takes as arguments the columns that we *don't want* to affect the grouping. So to get the value and year data for each group, we'll specify them as "non-grouping variables".

```python
country_unr_cubes = fi_and_swe_unemployment.groups('year', 'value')
```


The `country_unr_cubes` is now actually an iterator of filtered cubes for Finland and Sweden. We'll play with these a bit, so let's convert the iterator to a list so we don't lose the stuff.

```python
country_unr_cubes = list(country_unr_cubes)
```


With the cubes we can print out a table for the countries. Note that the columns are always in the same order, so we can get the header from either cube, and then just print the value-column as it's in the same order.

```python
print("Unemployments for finland and sweden over years")
years = country_unr_cubes[0].toColumns()['year']
print("Country\t\t" + "\t\t".join(years))
for country_unr in country_unr_cubes:
	columns = country_unr.toColumns()
	# Due to the 'collapsing', all grouping-fields, such as the
	# area, end up as just strings here.
	country = columns['area']
	formatted_values = ["%.3f"%f for f in columns['value']]
	print("%s\t\t%s"%(columns['area'], '\t\t'.join(formatted_values)))
```
```
Unemployments for finland and sweden over years
Country		2003		2004		2005		2006		2007		2008		2009		2010		2011		2012		2013		2014
FI		9.018		8.804		8.369		7.703		6.850		6.368		8.270		8.382		7.775		7.723		7.963		7.758
SE		6.566		7.373		7.652		7.054		6.127		6.184		8.306		8.373		7.504		7.652		7.913		7.604
```
