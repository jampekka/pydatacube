import itertools
import copy
import json
import re
import psycopg2
import psycopg2.extras
import pydatacube.pydatacube

class AlreadyExists(Exception): pass

class InvalidIdentifier(Exception): pass

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
		CREATE TABLE IF NOT EXISTS _dimension_categories (
			dataset_id VARCHAR(%i),
			dimension_id VARCHAR(%i),
			category_id VARCHAR(%i),
			category_label TEXT,
			PRIMARY KEY(dataset_id, dimension_id, category_id)
			)
		"""%(TABLE_ID_MAX_LEN, COLUMN_NAME_MAX_LEN,
			COLUMN_NAME_MAX_LEN))


class ResultIter(object):
	def __init__(self, dbresult):
		self.dbresult = dbresult
	
	def next(self):
		return self.dbresult.next()
	
	def __len__(self):
		return self.dbresult.rowcount
	
	def __iter__(self):
		return self

class SqlDataCube(object):
	@classmethod
	def Exists(cls, connection, id):
		c = connection.cursor()
		c.execute("SELECT COUNT(*) from _datasets WHERE id=%s", [id])
		return bool(c.fetchone()[0])

	@classmethod
	def FromCube(cls, connection, id, cube):
		spec = copy.deepcopy(cube.specification)
		if 'length' in spec:
			del spec['length']

		column_names = (dim['id'] for dim in spec['dimensions'])
		column_names = map(sql_name_cleanup, column_names)
		columns_query = ",".join("%s VARCHAR(255)"%s for s in column_names)
		table_name = sql_name_cleanup(id)[:TABLE_NAME_MAX_LEN]
		
		columns_query += ", _row_number serial"

		c = connection.cursor()
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

		category_labels = []
		for dim in spec['dimensions']:
			categories = dim.get('categories', [])
			for category in categories:
				category_labels.append(
					[id,
					dim['id'],
					category['id'],
					category.get('label', None)]
				)
		c.executemany("""
			INSERT INTO _dimension_categories
			(dataset_id, dimension_id, category_id, category_label)
			VALUES (%s, %s, %s, %s)""", category_labels)

		c.copy_from(CubeCsv(cube), table_name, columns=column_names)

		return cls(connection, id)


	def __init__(self, connection, id, filters={}):
		self._connection = connection
		self._id = id
		self._filters = filters
	
	@property
	def specification(self):
		# TODO: Should be probably cached.
		c = self._connection.cursor()
		try:
			c.execute("""
				SELECT specification FROM _datasets
				WHERE id=%s
				""", [self._id])
			spec = json.loads(c.fetchone()[0])
		finally:
			c.close()

		dims = {d['id']: d for d in spec['dimensions']}
		for dim_id, cat_ids in self._filters.iteritems():
			dim = dims[dim_id]
			dim['categories'] = [cat for cat in dim['categories']
				if cat['id'] in cat_ids]
		spec['length'] = len(self)
		return spec
	
	def filter(self, **kwargs):
		filters = copy.deepcopy(self._filters)
		# TODO: This allows filtering by categories, that may
		#	not actually be in a filtered object anymore,
		#	and thus breaks "materialized/filtered" equivalency.
		for dim_id, categories in kwargs.items():
			if isinstance(categories, basestring):
				categories = [categories]
			filters[dim_id] = set(categories)
		
		return SqlDataCube(self._connection, self._id, filters)
	
	def _get_where_clause(self):
		parts = []
		args = []
		for dim_id, cat_ids in self._filters.iteritems():
			values = []
			dim_id = verify_sql_name(dim_id)
			ph = ",".join(["%s"]*len(cat_ids))
			part = "%s IN (%s)"%(verify_sql_name(dim_id), ph)
			parts.append(part)
			args.extend(cat_ids)

		# SQL doesn't accept an empty WHERE-clause
		if len(parts) == 0:
			parts = ['1=1']
		return " AND ".join(parts), args
	
	def _get_table_name(self):
		c = self._connection.cursor()
		try:
			c.execute("""
				SELECT table_name FROM _datasets
				WHERE id=%s""", [self._id])
			table_name = verify_sql_name(c.fetchone()[0])
		finally:
			c.close()
		return table_name

	def _get_row_labels_query(self, start=0, end=None):
		# This is rather black magic to get the DB
		# to do the mapping from ids to labels.
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
		
		cols = []
		label_joins = []
		label_join_args = []
		for dim_id in dim_ids:
			label_join = """LEFT JOIN (
				SELECT
					category_label AS label,
					category_id AS id
				FROM _dimension_categories
				WHERE
					dataset_id=%%s AND
					dimension_id=%%s
				) _label_%(dim)s ON %(dim)s=_label_%(dim)s.id
				
				"""%dict(dim=dim_id)
			label_joins.append(label_join)
			label_join_args.append(self._id)
			label_join_args.append(dim_id)
			cols.append(
				"COALESCE(_label_%s.label, %s) AS %s"%(
					dim_id, dim_id, dim_id)
				)
				

		dim_ids = ",".join(dim_ids)
		from_query = " ".join([table_name] + label_joins)
		cols = ",".join(cols)
		args = label_join_args + args
		query = "SELECT %s FROM %s WHERE %s ORDER BY _row_number"%(
			cols, from_query, where)
		query += " OFFSET %s"
		args.append(start)
		if limit is not None:
			query += " LIMIT %s"
			args.append(limit)
		
		return query, args
	
	def _get_rows_query(self, start=0, end=None, category_labels=False):
		if category_labels:
			return self._get_row_labels_query(start, end)
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

		query = "SELECT %s FROM %s WHERE %s ORDER BY _row_number"%(
			dim_ids, table_name, where)
		query += " OFFSET %s"
		args.append(start)
		if limit is not None:
			query += " LIMIT %s"
			args.append(limit)
		
		return query, args

	def rows(self, start=0, end=None, category_labels=False):
		query, args = self._get_rows_query(start, end, category_labels)
		c = self._connection.cursor()
		c.execute(query, args)
		print c.mogrify(query, args)
		return ResultIter(c)

	def __iter__(self):
		return self.rows()
	
	def __getitem__(self, item):
		if not (hasattr(item, 'start') and
			hasattr(item, 'stop')):
			return object.__getitem__(item)
		if hasattr(item, 'step') and item.step != None:
			raise NotImplemented('Slice step not implemented')
		return self._get_iter(item.start, item.stop)
	
	def dimension_ids(self):
		return [d['id'] for d in self.specification['dimensions']]
	
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
	
	def toColumns(self, start=0, end=None, collapse_unique=True,
			category_labels=False, dimension_labels=False):
		dims = self.specification['dimensions']
		dim_ids = [d['id'] for d in dims]
		
		get_label = lambda x: x.get('label', x['id'])

		static_dims = []
		if collapse_unique:
			for d in dims:
				cats = d.get('categories', [])
				if len(cats) != 1:
					continue
				if category_labels:
					catval = get_label(cats[0])
				else:
					catval = cats[0]['id']

				static_dims.append(
					(d['id'], catval))
				dim_ids.remove(d['id'])

		col_qs = ['array_agg(%(d)s) as %(d)s'%dict(d=verify_sql_name(d))
			for d in dim_ids]
		rows_query, rows_args = self._get_rows_query(start, end,
						category_labels)
		
		query = "WITH rows_table AS (%s) SELECT %s FROM rows_table"%(
			rows_query,
			','.join(col_qs))
		args = rows_args

		c = self._connection.cursor(
			cursor_factory=psycopg2.extras.RealDictCursor
			)
		c.execute(query, args)
		result = c.fetchone()
		result.update(dict(static_dims))
		if dimension_labels:
			dim_labels = {d['id']: get_label(d) for d in dims}
			result = {dim_labels[k]: v for (k,v) in result.iteritems()}

		return result
	
	def group_for(self, *as_values):
		groups = set(self.dimension_ids()) - set(as_values)
		return self.group_by(*groups)
	
	def group_by(self, *grouping_dim_ids):
		dims = self.specification['dimensions']
		grouping_dims = [d for d in dims if d['id'] in grouping_dim_ids]
		normal_dims = [d for d in dims if d['id'] not in grouping_dim_ids]
		
		groupings = []
		for dim in grouping_dims:
			groupings.append([c['id'] for c in dim['categories']])
		
		grouping_dim_ids = [d['id'] for d in grouping_dims]
		for grouping in itertools.product(*groupings):
			yield self.filter(**dict(zip(grouping_dim_ids, grouping)))
	
	def _materialize(self):
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
		val_dim['values'] = [v[0] for v in c]
		spec['value_dimensions'] = [val_dim]
		return pydatacube.pydatacube._DataCube(spec)
		

class CubeCsv(object):
	def __init__(self, cube):
		self.cube_iter = iter(cube)

	
	def readline(self, *args):
		try:
			return "\t".join(map(str, self.cube_iter.next()))+"\n"
		except StopIteration:
			return ""
	
	read = readline
