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
		
		if 'namespace' in kwargs :
			nm = kwargs['namespace']
		else :
			nm = args[0]
		key = '%s-%s' % (cls.__name__, nm)
		
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
		#self.setReadOnly(readOnly)
		
		cur = self.connection.cursor()
		sql = "SELECT name FROM sqlite_master WHERE type='table'"
		cur.execute(sql)
		self.tables = set()
		for n in cur :
			self.tables.add(n[0])
		
		if not self.tableExits('rabalist_master') :
			sql = "CREATE TABLE rabalist_master (id INTEGER PRIMARY KEY AUTOINCREMENT, anchor_class NOT NULL, relation_name NOT NULL, table_name NOT NULL, length DEFAULT 0)"
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add('rabalist_master')
		
		if not self.tableExits('raba_tables_constraints') :
			sql = "CREATE TABLE raba_tables_constraints (table_name NOT NULL, constraints, PRIMARY KEY(table_name))"
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
			#print sql
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add(tableName)
	
	def registerRabalist(self, anchor_class_name, relation_name) :
		table_name = self.makeRabaListTableName(anchor_class_name, relation_name)
		
		self.createTable(table_name, 'id INTEGER PRIMARY KEY AUTOINCREMENT, anchor_id, value_or_id, type')
		
		sql = 'INSERT INTO rabalist_master (anchor_class, relation_name, table_name, length) VALUES (?, ?, ?, ?)'
		cur = self.connection.cursor()
		cur.execute(sql, (anchor_class_name, relation_name, table_name, 0))
		id = cur.lastrowid
		self.connection.commit()
		return id, table_name
	
	def unregisterRabalist(self, anchor_class_name, relation_name) :
		table_name = self.makeRabaListTableName(anchor_class_name, relation_name)
		#print 'unregistering', table_name
		self.dropTable(table_name)
		
		sql = 'DELETE FROM rabalist_master WHERE table_name = ?' 
		cur = self.connection.cursor()
		cur.execute(sql, (table_name, ))
		self.connection.commit()
	
	def makeRabaListTableName(self, anchor_class_name, relation_name) :
		return 'RabaList_%s_for_%s' % (relation_name, anchor_class_name)
		
	def updateRabaListLength(self, theId, newLength) :
		sql = "UPDATE rabalist_master SET length = ? WHERE id = ?"
		self.connection.cursor().execute(sql, (newLength, theId))
		self.connection.commit()

	def getRabaListInfos(self, theId = None, relation_name = None, anchor_class_name = None) :
		#you must provied either the id, of both relation_name and anchor_class_name
		if theId != None :
			sql = 'SELECT * FROM rabalist_master WHERE id = ?'
			cur = self.connection.cursor()
			cur.execute(sql, (theId, ))
		elif relation_name != None and anchor_class_name != None :
			sql = 'SELECT * FROM rabalist_master WHERE table_name = ?'
			cur = self.connection.cursor()
			cur.execute(sql, (self.makeRabaListTableName(anchor_class_name, relation_name), ))
		
		res = cur.fetchone()
		if res == None :
			return None
		return {'id' : res[0], 'anchor_class' : res[1], 'relation_name' : res[2], 'table_name' : res[3], 'length' : res[4]}
	
	def getRabaListTables(self) :
		sql = 'SELECT * FROM rabalist_master'
		cur = self.connection.cursor()
		cur.execute(sql)
		
		res = []
		for c in cur :
			res.append(c[0])
		
		return res
