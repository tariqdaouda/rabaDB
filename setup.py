import sqlite3 as sq
import os

RABA_CONNECTION = None
TYPES = None

def tableExits(name) :
	global RABA_CONNECTION
	sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" % name
	return RABA_CONNECTION.cursor().execute(sql).fetchone() != None
	
def init(rabaDBStorage = 'rabaDB') :
	if not os.path.exists(rabaDBStorage) :
		print "raba storage path %s not found, creating it..." % rabaDBStorage
		os.makedirs(rabaDBStorage)
	
def connect(dbName = 'rabaDB-0.db') :
	global RABA_CONNECTION
	RABA_CONNECTION = sq.connect(dbName)
	
	if not tableExits('Relations') :
		print "table Relations not found, creating it..."
		sql = 'CREATE TABLE Relations (id INTEGER PRIMARY KEY AUTOINCREMENT);'
		RABA_CONNECTION.cursor().execute(sql)
		RABA_CONNECTION.commit()

	return RABA_CONNECTION
	
def setTypes() :
	global TYPES
	TYPES = set()
	col = RABA_CONNECTION.cursor().execute('PRAGMA table_info(Relations)')
	for c in col :
		TYPES.add(c[1])
	return TYPES
	
def hasType(name) :
	global TYPES
	return name in TYPES
	
def addType(name, fields) :
	global TYPES
	if not hasType(name) :
		try :
			sql = 'ALTER TABLE Relations ADD %s;' % name
			RABA_CONNECTION.cursor().execute(sql)
			RABA_CONNECTION.commit()
		except sq.OperationalError as e :
			pass
			#if not e.message.startswith('duplicate column') :
			#	raise e
	
		sql = 'CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, %s);' % (name, ', '.join(list(fields)))
		print sql
		RABA_CONNECTION.cursor().execute(sql)
		RABA_CONNECTION.commit()
		
		TYPES.add(name)

def removeType(name) :
	"""Removes a type. SQLite doesn't support the drop of columns, this simply puts NULL in all
	the values corresponding to the type in Relations Table and drops the table associated with"""
	global TYPES
	if hasType(name) :
		sql = 'UPDATE Relations SET %s=NULL WHERE 1;' % name
		RABA_CONNECTION.cursor().execute(sql)
		RABA_CONNECTION.commit()

		sql = 'DROP TABLE IF EXISTS %s;' % name
		RABA_CONNECTION.cursor().execute(sql)
		RABA_CONNECTION.commit()
		
		TYPES.remove(name)	

def autoclean() :
	"""TODO: Copies the Relations table into a new one removing all the collumns that have all their values to NULL
	and drop the tables that correspond to these tables"""
	#TODO
	pass


init()
RABA_CONNECTION = connect()
setTypes()
