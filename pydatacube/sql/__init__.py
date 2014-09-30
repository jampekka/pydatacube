import itertools
import copy
import json
import re
#import psycopg2
#import psycopg2.extras
import pydatacube.pydatacube
from pydatacube import method_memoize, lazy_dict, DataCubeBase

class NotFound(Exception): pass

class AlreadyExists(Exception): pass

class InvalidIdentifier(Exception): pass

class IncompatibleCubes(Exception): pass


def verify_sql_name(name):
	if re.match('[^0-9a-zA-Z]', name):
		raise InvalidIdentifier("Sql name '%s' contains illegal characters"%name)
	return name

def sql_name_cleanup(name):
	return re.sub('[^0-9a-zA-Z]+', '_', name)

TABLE_ID_MAX_LEN = 255
TABLE_NAME_MAX_LEN = 50
COLUMN_NAME_MAX_LEN = 50

def initialize_schema(connection):
	c = connection.cursor()
	# TODO: No need to duplicate column info in
	#	the specification
	c.execute("""
		CREATE TABLE IF NOT EXISTS _datasets (
			id VARCHAR(%i) PRIMARY KEY,
			table_name VARCHAR(%i) NOT NULL,
			specification TEXT NOT NULL,
			cube_value_column VARCHAR(%i)
			)
		"""%(TABLE_ID_MAX_LEN, TABLE_NAME_MAX_LEN, COLUMN_NAME_MAX_LEN))
	
	c.execute("""
		CREATE TABLE IF NOT EXISTS _dataset_dimensions (
			dataset_id VARCHAR(%i),
			dimension_id VARCHAR(%i),
			dimension_label TEXT,
			PRIMARY KEY(dataset_id, dimension_id)
			)
		"""%(TABLE_ID_MAX_LEN, COLUMN_NAME_MAX_LEN))
	
	c.execute("""
		CREATE TABLE IF NOT EXISTS _categories (
			surrogate serial PRIMARY KEY,
			name TEXT,
			label TEXT
		)""")
	

	c.execute("""
		CREATE TABLE IF NOT EXISTS _dimension_categories (
			dataset_id VARCHAR(%i),
			dimension_id VARCHAR(%i),
			category_surrogate INTEGER REFERENCES _categories(surrogate),
			PRIMARY KEY(dataset_id, dimension_id, category_surrogate)
			)
		"""%(TABLE_ID_MAX_LEN, COLUMN_NAME_MAX_LEN))


class ResultIter(object):
	def __init__(self, dbresult):
		self.dbresult = dbresult
		self.dbresult_iter = iter(dbresult)
	
	def next(self):
		return self.dbresult_iter.next()
	
	def __len__(self):
		return self.dbresult.rowcount
	
	def __iter__(self):
		return self

class SqlDataCube(DataCubeBase):
	@classmethod
	def Exists(cls, connection, id):
		c = connection.cursor()
		c.execute("SELECT COUNT(*) from _datasets WHERE id=%s", [id])
		return bool(c.fetchone()[0])
	
	@classmethod
	def Remove(cls, connection, id):
		if not cls.Exists(connection, id):
			return
		cube = cls(connection, id)
		c = connection.cursor()
		c.execute("DROP TABLE %s"%(cube._get_table_name()))
		c.execute("DELETE from _dataset_dimensions WHERE dataset_id=%s", [id])
		c.execute("DELETE from _datasets WHERE id=%s", [id])
		

	@classmethod
	def FromCube(cls, connection, id, cube, replace=False):
		if replace:
			cls.Remove(connection, id)

		spec = copy.deepcopy(cube.specification)
		if 'length' in spec:
			del spec['length']
		
		c = connection.cursor()
		columns_query = []
		column_names = []
		column_mappings = []
		category_columns = []
		for dim in spec['dimensions']:
			name = sql_name_cleanup(dim['id'])
			column_names.append(name)
			if 'categories' not in dim:
				columns_query.append("%s VARCHAR(255)"%name)
				column_mappings.append({})
				continue
			
			category_columns.append(name)
			columns_query.append("%s INTEGER"%name)
			ids = []
			surrogates = []
			for cat in dim['categories']:
				# Executemany's returning is broken :(
				c.execute("""
				INSERT INTO _categories
				(name, label) VALUES (%s, %s)
				RETURNING surrogate""",
				[cat['id'], cat.get('label', None)])
				ids.append(cat['id'])
				surrogates.append(c.fetchone()[0])

				c.execute("""
				INSERT INTO _dimension_categories
				(dataset_id, dimension_id, category_surrogate)
				VALUES
				(%s, %s, %s)
				""", (id, name, surrogates[-1]))
			mapping = dict(zip(ids, surrogates))
			column_mappings.append(mapping)
				
				

		columns_query = ",".join(columns_query)
		table_name = sql_name_cleanup(id)[:TABLE_NAME_MAX_LEN]
		
		columns_query += ", _row_number serial"

		create_query = "CREATE TABLE %s (%s)"%(table_name, columns_query)
		c.execute(create_query)
		dims = cube.specification['dimensions']
		value_dims = [d['id'] for d in dims if 'categories' not in d]
		if value_dims == ['value']:
			cube_value_column = 'value'
		else:
			cube_value_column = None

		c.execute("""
			INSERT INTO _datasets
			(id, table_name, specification, cube_value_column) VALUES
			(%s, %s, %s, %s)""",
			[id, table_name, json.dumps(spec), cube_value_column])
		

		dimension_labels = [[id, d['id'], d.get('label', None)]
			for d in spec['dimensions']]
		c.executemany("""
			INSERT INTO _dataset_dimensions
			(dataset_id, dimension_id, dimension_label)
			VALUES (%s, %s, %s)""", dimension_labels)

		# MySQL hacks
#		dumpfile = open('/tmp/sqlstatcube_dump.csv', 'w')
#		for row in cube:
#			dumpfile.write(",".join(row))
#			dumpfile.write('\n')
#		dumpfile.close()
#		c.execute("""LOAD DATA INFILE '/tmp/sqlstatcube_dump.csv' INTO TABLE %s
#			FIELDS TERMINATED BY ','"""%table_name)
		fake_csv = CubeMappingCsv(cube, column_mappings)
		c.copy_from(fake_csv, table_name, columns=column_names)

		# Create index on the row number column, which speeds
		# ORDER BY -operations of large queries a lot by avoiding
		# disk sorts.
		c.execute("""
			CREATE INDEX ON %s (_row_number)
			"""%(table_name))

		for col in category_columns:
			c.execute("""
			CREATE INDEX ON %s (%s)
			"""%(table_name, col))

		return cls(connection, id)


	def __init__(self, connection, id, filters={}):
		self._connection = connection
		self._id = id
		self._filters = filters
	
	@property
	@method_memoize
	def specification(self):
		c = self._connection.cursor()
		try:
			c.execute("""
				SELECT specification FROM _datasets
				WHERE id=%s
				""", [self._id])
			
			result = c.fetchone()
			if not result:
				raise NotFound("No cube with id %s"%self._id)
			
			spec = json.loads(result[0])
		finally:
			c.close()

		dims = {d['id']: d for d in spec['dimensions']}
		for dim_id, cat_ids in self._filters.iteritems():
			dim = dims[dim_id]
			dim['categories'] = [cat for cat in dim['categories']
				if cat['id'] in cat_ids]
		
		spec = lazy_dict(spec)
		spec.lazy['length'] = lambda x: len(self)

		return spec
	
	@property
	def metadata(self):
		return self.specification['metadata']
	
	def filter(self, **kwargs):
		filters = copy.deepcopy(self._filters)
		# TODO: This allows filtering by categories, that may
		#	not actually be in a filtered object anymore,
		#	and thus breaks "materialized/filtered" equivalency.
		# TODO: We could calculate the new categories and dimensions
		#	here (even lazily) and pass them to the filtered
		#	cube, saving a query. This gets expensive especially
		#	in group_by.
		for dim_id, categories in kwargs.items():
			if isinstance(categories, basestring):
				categories = [categories]
			filters[dim_id] = set(categories)
		
		new = SqlDataCube(self._connection, self._id, filters)
		if hasattr(self, '_table_name_cache'):
			new._table_name_cache = self._table_name_cache
		
		return new

	def _get_where_clause(self):
		parts = []
		args = []
		for dim_id, cat_ids in self._filters.iteritems():
			values = []
			dim_id = verify_sql_name(dim_id)
			ph = ",".join(["%s"]*len(cat_ids))
			part = "%s IN (SELECT surrogate from _categories WHERE name IN (%s))"%(verify_sql_name(dim_id), ph)
			parts.append(part)
			args.extend(cat_ids)

		# SQL doesn't accept an empty WHERE-clause
		if len(parts) == 0:
			parts = ['1=1']
		return " AND ".join(parts), args
	
	def _get_table_name(self):
		if hasattr(self, '_table_name_cache'):
			return self._table_name_cache

		c = self._connection.cursor()
		try:
			c.execute("""
				SELECT table_name FROM _datasets
				WHERE id=%s""", [self._id])
			table_name = verify_sql_name(c.fetchone()[0])
		finally:
			c.close()
		self._table_name_cache = table_name
		return table_name

	def _get_row_labels_query(self, start=0, end=None, order=[]):
		id_rows, args = self._get_row_ids_query(start, end, order=order)
		mapping = """(SELECT COALESCE(label, name) FROM _categories
				WHERE _categories.surrogate=rows.%(dim_id)s)
				as %(dim_id)s"""

		return self._get_mapping_query((id_rows, args), mapping)
	
	def _get_rows_query(self, start=0, end=None, category_labels=False, order=[]):
		if category_labels:
			return self._get_row_labels_query(start, end, order=order)
		id_rows, args = self._get_row_ids_query(start, end, order=order)
		mapping = """(SELECT name FROM _categories
				WHERE _categories.surrogate=rows.%(dim_id)s)
				as %(dim_id)s"""

		return self._get_mapping_query((id_rows, args), mapping)
		
	def _get_mapping_query(self, (id_rows, args), mapping):
		table_name = self._get_table_name()
			
		mappings = []
		for dim in self.specification['dimensions']:
			dim_id = verify_sql_name(dim['id'])
			if 'categories' not in dim:
				mappings.append(dim_id)
				continue

			mappings.append(mapping%dict(dim_id=dim_id))
		cols = ",".join(mappings)
		
		# The query is marginally (~10%) faster on very large
		# queries (eg. CSV dumps) if we
		# preselect the categories like this, but probably
		# not worth the complexity and inflexibilty.
		# NOTE: At least with PostgreSQL 9.3, this should
		#	be in the FROM-clause and the rows in a CTE,
		#	otherwise the performance is horrible.
		#cats_q = """SELECT _categories.* FROM _categories
		#	JOIN _dimension_categories
		#	ON category_surrogate=surrogate
		#	WHERE dataset_id=%s"""
		#args.append(self._id)


		query = """
		SELECT %s FROM (%s) rows
		"""%(cols, id_rows)
		return query, args

		
	
	def _get_row_ids_query(self, start=0, end=None, order=[]):
		table_name = self._get_table_name()
		where, args = self._get_where_clause()
		if start is None:
			start = 0
		if start < 0:
			raise ValueError('Start must be >= 0')
		if end is not None and end < start:
			raise ValueError('End must be >= start')
		
		limit = None
		if end is not None:
			limit = end - start 
		
		dim_ids = [verify_sql_name(dim['id'])
			for dim in self.specification['dimensions']]

		dim_ids = ",".join(dim_ids)
		
		order = map(verify_sql_name, order)
		order = ', '.join(order)
		if len(order) > 0:
			order += ","
		query = "SELECT %s FROM %s WHERE %s ORDER BY %s _row_number"
		query = query%(dim_ids, table_name, where, order)
		
		# MySQL hack, as it doesn't support OFFSET
		# without LIMIT
		#if limit is None:
		#	limit = 18446744073709551615
		if limit is not None:
			query += " LIMIT %s"
			args.append(limit)
		query += " OFFSET %s"
		args.append(start)

		return query, args

	def rows(self, start=0, end=None, category_labels=False, order=[]):
		query, args = self._get_rows_query(start, end, category_labels, order=order)
		c = self._connection.cursor()
		c.execute(query, args)
		return ResultIter(c)

	def __getitem__(self, item):
		if not (hasattr(item, 'start') and
			hasattr(item, 'stop')):
			return object.__getitem__(item)
		if hasattr(item, 'step') and item.step != None:
			raise NotImplemented('Slice step not implemented')
		return self._get_iter(item.start, item.stop)
	
	@method_memoize
	def __len__(self):
		c = self._connection.cursor()
		try:
			table_name = self._get_table_name()
			where, args = self._get_where_clause()
			query = "SELECT COUNT(*) FROM %s WHERE %s"%(
				table_name, where)
			c.execute(query, args)
			return c.fetchone()[0]
		finally:
			c.close()
	
	def group_by(self, *grouping_dim_ids):
		dims = self.specification['dimensions']
		grouping_dims = [(i, d) for i, d in enumerate(dims) if d['id'] in grouping_dim_ids]
		grouping_dim_idx, grouping_dims = zip(*grouping_dims)
		normal_dims = [d for d in dims if d['id'] not in grouping_dim_ids]

		groupings = []
		for dim in grouping_dims:
			groupings.append([c['id'] for c in dim['categories']])
		
		grouping_dim_ids = [d['id'] for d in grouping_dims]
		c = self._connection.cursor()
		cols = map(verify_sql_name, grouping_dim_ids)
		where, args = self._get_where_clause()
		c.execute("SELECT COUNT(DISTINCT(%s)) FROM %s WHERE %s"%(
			",".join(cols), self._get_table_name(), where),
			args)
		n_groups = c.fetchone()[0]
		grp_iter = self.__iter_groups(grouping_dim_ids, grouping_dim_idx, groupings)
		return LengthIterator(grp_iter, n_groups)

	def __iter_groups(self, grouping_dim_ids, grouping_dim_idx, groupings):
		# TODO: Any chance (or reason) for doing this in SQL?
		rows = self.rows(order=grouping_dim_ids)
		getkey = lambda row: tuple(row[i] for i in grouping_dim_idx)
		groups = itertools.groupby(rows, getkey)
		for grouping, data in groups:
			filter = dict(zip(grouping_dim_ids, grouping))
			subcube = self.filter(**filter)
			yield _populate_cube(subcube, data)
	
	def _materialize(self, allow_value_iterator=False):
		c = self._connection.cursor()
		c.execute("SELECT cube_value_column FROM _datasets WHERE id=%s",
			[self._id])
		value_col = c.fetchone()[0]
		if value_col is None:
			raise NotImplementedError("Can't materialize this type of cube")
		spec = copy.deepcopy(self.specification)
		val_dim, val_dim_i = ((dim, i)
			for i, dim in enumerate(spec['dimensions'])
			if dim['id'] == value_col).next()
		del spec['dimensions'][val_dim_i]
		
		where_clause, args = self._get_where_clause()
		

		q = "SELECT %s FROM %s WHERE %s ORDER BY _row_number"%(
			verify_sql_name(value_col), self._get_table_name(),
			where_clause)
		c = self._connection.cursor()
		c.execute(q, args)

		# This way, with a proper encoder, the output can
		# be streamed instead of read into memory
		if allow_value_iterator:
			val_dim['values'] = (v[0] for v in c)
		val_dim['values'] = [v[0] for v in c]
		spec['value_dimensions'] = [val_dim]
		return pydatacube.pydatacube._DataCube(spec)
	
	def _value_dimension_values(self):
		c = self._connection.cursor()
		c.execute("SELECT cube_value_column FROM _datasets WHERE id=%s",
			[self._id])
		value_col = c.fetchone()[0]
		if value_col is None:
			raise NotImplementedError("The cube doesn't have an ordered single value column")
			
		where_clause, args = self._get_where_clause()

		q = "SELECT %s FROM %s WHERE %s ORDER BY _row_number"%(
			verify_sql_name(value_col), self._get_table_name(),
			where_clause)
		c = self._connection.cursor()
		c.execute(q, args)
		return [r[0] for r in c]

	
	def dump_csv(self, output):
		c = self._connection.cursor()
		query = "(%s)"%c.mogrify(*self._get_rows_query())
		c.copy_to(output, query, sep=',')

def _populate_cube(self, data):
	# How I love Python! Try to do this with your nazi static typing
	# without changing the original class etc ugliness!
	orig_rows = self.rows
	def rows(start=0, end=None, category_labels=False):
		if category_labels:
			raise NotImplemented("Category labels queries not implemented (it was a stupid idea anyway)")
		for row in itertools.islice(data, start, end):
			yield row
		self.rows = orig_rows

	self.rows = rows

	return self



class LengthIterator(object):
	def __init__(self, itr, length):
		self.itr = itr
		self.length = length
	
	def __iter__(self):
		return iter(self.itr)

	def __len__(self):
		return self.length

class CubeCsv(object):
	def __init__(self, cube):
		self.cube_iter = iter(cube)

	
	def readline(self, *args):
		try:
			return "\t".join(map(str, self.cube_iter.next()))+"\n"
		except StopIteration:
			return ""
	
	read = readline

class CubeMappingCsv(object):
	def __init__(self, cube, mappings):
		self.cube_iter = iter(cube)
		self.mappings = mappings
	
	def readline(self, *args):
		try:
			row = self.cube_iter.next()
		except StopIteration:
			return ""

		row = [m.get(v, v) for m, v in zip(self.mappings, row)]
		return "\t".join(map(str, row))+"\n"
		
	read = readline

