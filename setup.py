import sqlite3 as sq
import os

#class RabaSingleton(type):
#	_instances = {}
#	def __call__(cls, *args, **kwargs):
#		if cls not in cls._instances:
#			cls._instances[cls] = super(RabaSingleton, cls).__call__(*args, **kwargs)
#		
#		return cls._instances[cls]


class RabaNameSpaceSingleton(type):
	_instances = {}
	def __call__(cls, *args, **kwargs):
		if len(args) < 1 :
			raise ValueError('The first argument to %s must be a namespace' % cls.__name__)
		key = '%s-%s' % (cls.__name__, args[0])
		if key not in cls._instances:
			cls._instances[key] = super(RabaNameSpaceSingleton, cls).__call__(*args, **kwargs)
		return cls._instances[key]

class RabaConfiguration(object) :
	"""This class must be instanciated at the begining of the script just after the import of setup giving it the path to the the DB file. ex : 
	
	from rabaDB.setup import *
	RabaConfiguration(namespace, './dbTest.db')
	
	After the first instanciation you can call it without parameters. As this class is a Singleton, it will always return the same instance"""
	__metaclass__ = RabaNameSpaceSingleton
	
	def __init__(self, namespace, dbFile = None) :
		if dbFile == None :
			raise ValueError("""No configuration detected for namespace '%s'.
			Have you forgotten to add: %s('%s', 'the path to you db file') just after the import of setup?""" % (namespace, self.__class__.__name__, namespace))
		self.dbFile = dbFile
		self.loadedRabaClasses = {}
	
	def registerRabaClass(self, cls) :
		self.loadedRabaClasses[cls.__name__] = cls
	
	def getClass(self, name) :
		return self.loadedRabaClasses[name]
	
class RabaConnection(object) :
	"""A class that manages the connection to the sqlite3 database. Don't be afraid to call RabaConnection() as much as you want"""
	
	__metaclass__ = RabaNameSpaceSingleton
	
	def __init__(self, namespace) :
		#conf = confParser('rabaDB.conf')
		self.connection = sq.connect(RabaConfiguration(namespace).dbFile)
		
		cur = self.connection.cursor()
		sql = "SELECT name FROM sqlite_master WHERE type='table'"
		cur.execute(sql)
		self.tables = set()
		for n in cur :
			self.tables.add(n[0])
		
		if not self.tableExits('rabalist_master') :
			sql = "CREATE TABLE rabalist_master (anchor_type, relation_name, table_name NOT NULL, PRIMARY KEY(anchor_type, relation_name))"
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add('rabalist_master')
		
		if not self.tableExits('raba_tables_constraints') :
			sql = "CREATE TABLE raba_tables_constraints (table_name, constraints, PRIMARY KEY(table_name))"
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add('raba_tables_constraints')
		
	def __getattr__(self, name):
		return self.connection.__getattribute__(name)
		
	def tableExits(self, name) :
		return name in self.tables

	def dropTable(self, name) :
		if self.tableExits(name) :
			sql = "DROP TABLE IF EXISTS %s" % name
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.remove(name)
		
	def createTable(self, tableName, strFields) :
		if not self.tableExits(tableName) :
			sql = 'CREATE TABLE %s ( %s)' % (tableName, strFields)
			print sql
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add(tableName)
			
	def registerRabalistTable(self, anchor_type, relation_name, table_name) :
		sql = 'INSERT INTO rabalist_master (anchor_type, relation_name, table_name) VALUES (?, ?, ?)'
		self.connection.cursor().execute(sql, (anchor_type.__name__, relation_name, table_name))
		self.connection.commit()
	
	def unregisterRabaListTable(self, anchor_type, relation_name) :
		sql = 'DELETE FROM rabalist_master WHERE anchor_type = ? AND relation_name = ?'
		self.connection.cursor().execute(sql, (anchor_type.__name__, relation_name))
		self.connection.commit()
	
	def getRabaListTableName(self, anchor_type, relation_name) :
		sql = 'SELECT table_name FROM rabalist_master WHERE anchor_type = ? AND relation_name = ?'
		cur = self.connection.cursor()
		cur.execute(sql, (anchor_type.__name__, relation_name))
		res = cur.fetchone()
		if res != None :
			return res[0]
		else :
			return None

	def getRabaListTables(self) :
		sql = 'SELECT table_name FROM rabalist_master WHERE 1'
		cur = self.connection.cursor()
		cur.execute(sql)
		
		res = []
		for c in cur :
			res.append(c[0])
		
		return res 
