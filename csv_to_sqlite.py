import sqlite3

def csv_to_sqlite(csv_path, sep, headers=None):
	table_name = "raw_data"
	
	conn = sqlite3.connect(csv_path+'.sqlite')
	curs = conn.cursor()
	
	sql = "ATTACH DATABASE ? AS compact"
	curs.execute( sql, ( csv_path+'.compact.sqlite', ) )
	
	def init_table(curs, table_name, cols):
		print("table", table_name, "#cols", len(cols))
		
		sql = "DROP TABLE IF EXISTS %s " % ( table_name, )
		curs.execute( sql )
		
		sql = "CREATE TABLE IF NOT EXISTS %s ( '%s' )" % ( table_name, "', '".join(cols), )
		print(sql)
		curs.execute( sql )
		
		sql = "INSERT INTO %s VALUES ( %s )" % ( table_name, ", ".join([ "?" for h in cols]), )
		return sql

	def populate_raw_table(headers):
		print("populate_raw_table")
		with open(csv_path, "r") as f: 
			
			for i, row in enumerate(f):
				if row[-1] == "\n":
					row = row[:-1] # last value has a trailing '\n'
				row = row.split(sep)
				
				if i == 0:
					if isinstance(headers, list):
						# explicit headers
						cols = headers
					elif headers == True:
						# table has headers in data
						cols = row
					else:
						# no headers
						cols = [ "col%s" % i for i in range(len(row)) ]
					
					sql = init_table( curs, "raw_data", cols)
					
					if headers == True:
						# first row consumed as header
						continue
					
				curs.execute(sql, row)
		
				#if i>250000: break
				
		curs.connection.commit()
		print("Raw table created\n")

	
	def populate_compact_table():
		print("populate_compact_table")

		sql = "SELECT * FROM %s LIMIT 1" % ( table_name, )
		curs.execute( sql )
		cols = [ desc[0] for desc in curs.description ]
		print("cols", cols)
		
		sql = "SELECT COUNT( * ) FROM %s" % ( table_name, )
		curs.execute( sql )
		data = curs.fetchone()
		nbRows = data[0]
		print("#rows", nbRows)

		# register a function in SQLite
		column_value_to_index_map = {}
		def column_value_to_index(column_name, column_value):
			return column_value_to_index_map[column_name][column_value]
		conn.create_function("column_value_to_index", 2, column_value_to_index)
		
		cols_transfo = []
		for i, col in enumerate(cols):
			
			sql = "SELECT SUM( CAST( %s AS INTEGER) IS NOT %s ) FROM %s" % ( col, col, table_name, )
			curs.execute( sql )
			data = curs.fetchone()
			integerOnly = data[0] == 0
			
			sql = "SELECT COUNT( DISTINCT %s ) FROM %s" % ( col, table_name, )
			curs.execute( sql )
			data = curs.fetchone()
			nbUniqueValue = data[0]
				
			if integerOnly:
				print("-", col, "#UniqueValue", nbUniqueValue, "INTEGER VALUES ONLY")
				cols_transfo.append("CAST( %s AS INTEGER)" % ( col, ))
				continue
			
			elif nbUniqueValue < ( nbRows // 20 ):
				sql = "SELECT DISTINCT %s FROM %s" % ( col, table_name, )
				curs.execute( sql )
				data = [ row[0] for row in curs.fetchall() ]
				column_value_to_index_map[col] = { val: i+1 for i, val in enumerate(data) }
							
				sql = "DROP TABLE IF EXISTS compact.%s_%s_id " % ( table_name, col, )
				curs.execute( sql )
				
				sql = "CREATE TABLE IF NOT EXISTS compact.%s_%s_id ( id int primary key, name )" % ( table_name, col, )
				curs.execute( sql )
				
				sql = "INSERT INTO compact.%s_%s_id VALUES ( ?, ? )" % ( table_name, col, )
				curs.executemany( sql, [ ( i+1, val ) for i, val in enumerate(data) ] )
			
				cols_transfo.append("column_value_to_index('%s', %s) as %s" % ( col, col, col, ))
				print("-", col, "#UniqueValue", nbUniqueValue, data if len(data)<20 else "[...]")
				
			else:
				cols_transfo.append("%s" % ( col, ))
				print("-", col, "#UniqueValue", nbUniqueValue, "TOO MANY UNIQUE VALUES")
		
		curs.connection.commit()
		
		init_table( curs, "compact.raw_data", cols)
		sql = "INSERT INTO compact.%s SELECT %s FROM %s" % ( table_name, ", ".join(cols_transfo), table_name, )
		curs.execute( sql )

		curs.connection.commit()
		print("Compact table created\n")


	populate_raw_table(headers)
	
	populate_compact_table()	

def read_sql(curs, sql, with_headers=True):
	curs.execute( sql )
	cols = [ desc[0] for desc in curs.description ]
	data = curs.fetchall()
	if with_headers:
		return [ cols ] + data
	else:
		return data

def table_to_text(data):
	return "\n".join([ "\t".join([ str(x) for x in row ]) for row in data ])

def print_sqlite(db_path, limit = 25):
	conn = sqlite3.connect(db_path)
	curs = conn.cursor()
	
	sqlite_master = read_sql(curs, "select * from sqlite_master where type='table' ")
	tables = [ row[1] for row in sqlite_master[1:] ] 
	print("sqlite_master")
	print(table_to_text(sqlite_master), "\n")
	
	for table_name in tables: 
		table_size = read_sql(curs, "select count(*) from %s" % ( table_name, )) [1][0]
		table_data = read_sql(curs, "select * from %s limit %i" % ( table_name, limit, ))
		print(table_name, "#rows", table_size)
		print(table_to_text(table_data), "\n")
	
csv_to_sqlite("GooglePresCleanData.out", ",", [ "TestSuite", "ChangeRequest", "TestStage", "TestStatus", "LaunchTime", "ExecutionTimeMs", "TestSize", "NumShards", "NumRuns", "Language" ])

print_sqlite("GooglePresCleanData.out.sqlite")
print_sqlite("GooglePresCleanData.out.compact.sqlite")

		
