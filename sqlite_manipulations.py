import sqlite3
import csv_to_sqlite
	
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


def google_analyse_1():
	"""
	All TestSuite names are starting witht the same 130 chars root_path :
	/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/ 
	
	split(TestSuite, '/', 1)
	e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/ 	
	"""
	
	conn = sqlite3.connect("GooglePresCleanData.out.compact.sqlite")
	curs = conn.cursor()

	def split(txt, sep, idx):
		if not isinstance(txt, str):
			return None
		tmp = txt.split(sep)
		if idx<len(tmp):
			if (idx+1) < len(tmp):
				return tmp[idx] + sep
			else:
				return tmp[idx]
		return ""
	conn.create_function("split", 3, split)

	col_name = "TestSuite"
	col_name = "name"
	
	#table_data = read_sql(curs, "select distincT( SUBSTR(TestSuite, 0, 180 + INSTR(SUBSTR(TestSuite, 180), '/'))) from raw_data_TestSuite  limit 500")

	table_data = read_sql(curs, "select %s from raw_data_TestSuite limit 5" % ( col_name, ))
	print(table_to_text(table_data), "\n")
	
	table_data = read_sql(curs, "select distinct split(%s, '/', 1)  from raw_data_TestSuite limit 500" % ( col_name, ))
	print(table_to_text(table_data), "\n")
	
	table_data = read_sql(curs, "select distinct split(%s, '/', 1) || split(%s, '/', 2)  from raw_data_TestSuite limit 500" % ( col_name, col_name, ))
	print(table_to_text(table_data), "\n")
	
	table_data = read_sql(curs, "select distinct split(%s, '/', 1) || split(%s, '/', 2) || split(%s, '/', 3) from raw_data_TestSuite limit 5000" % ( col_name, col_name, col_name, ))
	print(table_to_text(table_data), "\n")


def google_transfo_1():
	curs = conn.cursor()

	sql = "UPDATE raw_data_TestSuite SET TestSuite = substr(TestSuite, 130)" 
	curs.execute( sql )
	curs.connection.commit()
	
	sql = "VACUUM" 
	curs.execute( sql )
	curs.connection.commit()



def rail_analyse_1():
	conn = sqlite3.connect("RailsCleanData.out.compact.sqlite")
	curs = conn.cursor()
	
	table_data = read_sql(curs, "select * from raw_data limit 25")
	print(table_to_text(table_data), "\n")
	
	table_data = read_sql(curs, "select count(*) from raw_data ")
	print(table_to_text(table_data), "\n")
	
	table_data = read_sql(curs, "select count(distinct JobId) from raw_data ")
	print(table_to_text(table_data), "\n")
	
	sql = "select JobId, max(JobStartTime) - min(JobStartTime) as JobStartTimes, max(JobAllowFailure) - min(JobAllowFailure) as JobAllowFailures from raw_data group by JobId"
	sql = "select * from ( " + sql + " ) as T where JobStartTimes <> 0 or JobAllowFailures <> 0"
	table_data = read_sql(curs, sql)
	print(sql)
	print(table_to_text(table_data), "\n")
	
	table_data = read_sql(curs, "select count(distinct BuildNumber) from raw_data ")
	print(table_to_text(table_data), "\n")
	
	cols = [ "BuildIsPullRequest", "CommitSha", "BuildState", "BuildStartTime", "BuildFinishTime", "BuildDuration" ]
	cols_select = ", ".join([ "max(%s) - min(%s) as %ss" % ( x, x, x, ) for x in cols ])
	cols_where = " or ".join([ "%ss <> 0" % ( x,  ) for x in cols ])
	sql = "select BuildNumber, %s from raw_data group by BuildNumber" % ( cols_select, )
	sql = "select * from ( %s ) as T where %s" % ( sql, cols_where, )
	table_data = read_sql(curs, sql)
	print(sql)
	print(table_to_text(table_data), "\n")
	

def rail_transfo_1():
	# Run further normalisation 
	
	conn = sqlite3.connect("RailsCleanData.out.compact.sqlite")
	curs = conn.cursor()
	
	sql = "ATTACH DATABASE ? AS compact"
	curs.execute( sql, ( 'RailsCleanData.out.compact2.sqlite', ) )


	sql = "DROP TABLE IF EXISTS compact.raw_data_Job " 
	curs.execute( sql )

	sql = "CREATE TABLE IF NOT EXISTS compact.raw_data_Job ( id int primary key, JobStartTime, JobAllowFailure )" 
	curs.execute( sql )
	
	sql = "INSERT INTO compact.raw_data_Job SELECT JobId, JobStartTime, JobAllowFailure FROM raw_data GROUP BY JobId"
	curs.execute( sql )


	sql = "DROP TABLE IF EXISTS compact.raw_data_Build " 
	curs.execute( sql )
	
	sql = "CREATE TABLE IF NOT EXISTS compact.raw_data_Build ( BuildNumber int primary key, BuildIsPullRequest, CommitSha, BuildState, BuildStartTime, BuildFinishTime, BuildDuration )" 
	curs.execute( sql )
	
	sql = "INSERT INTO compact.raw_data_Build SELECT BuildNumber, BuildIsPullRequest, CommitSha, BuildState, BuildStartTime, BuildFinishTime, BuildDuration FROM raw_data GROUP BY BuildNumber"
	curs.execute( sql )

	# Better working in same DB and drop normalised columns
	# (otherwise should also copy all other tables)
	
	sql = "DROP TABLE IF EXISTS compact.raw_data " 
	curs.execute( sql )

	sql = "CREATE TABLE IF NOT EXISTS compact.raw_data ( TestSuite, LaunchTime, ExecutionTimeSec, NumRuns, NumAssertions, NumFailures, NumErrors, NumSkips, BuildNumber, JobId )" 
	curs.execute( sql )

	sql = "INSERT INTO compact.raw_data SELECT TestSuite, LaunchTime, ExecutionTimeSec, NumRuns, NumAssertions, NumFailures, NumErrors, NumSkips, BuildNumber, JobId FROM raw_data "
	curs.execute( sql )
	
	
	curs.connection.commit()


def rail_analyse_2():
	db = "RailsCleanData.out.compact.sqlite"
	conn = sqlite3.connect(db)
	curs = conn.cursor()
	
	#csv_to_sqlite.print_sqlite(db)
	
	sql = "select count(distinct BuildNumber) from raw_data "
	table_data = read_sql(curs, sql)
	print(sql)
	print(table_to_text(table_data), "\n")
	
	# real commits
	sql = "select count(distinct BuildNumber) from raw_data where BuildIsPullRequest <> 1 "
	table_data = read_sql(curs, sql)
	print(sql)
	print(table_to_text(table_data), "\n")
	
	sql = "select BuildNumber, sum(NumFailures) from raw_data where BuildIsPullRequest <> 1 group by BuildNumber"
	table_data = read_sql(curs, sql)
	print(sql)
	print(table_to_text(table_data), "\n")
	
	# pull requests
	sql = "select BuildNumber, sum(NumFailures) from raw_data where BuildIsPullRequest <> 2 group by BuildNumber"
	table_data = read_sql(curs, sql)
	print(sql)
	print(table_to_text(table_data), "\n")
	

#google_analyse_1()
#google_transfo_1()


# rail_analyse_1()
# rail_transfo_1()
rail_analyse_2()