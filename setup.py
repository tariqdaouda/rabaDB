import sqlite3 as sq
import os

class Singleton(type):
	_instances = {}
	def __call__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
		return cls._instances[cls]

class RabaConnection :
	__metaclass__ = Singleton
	def __init__(self, dbFileName = '/u/daoudat/usr/lib/python/rabaDB/rabaDB-0.db') :
		self.connection = sq.connect(dbFileName)
		
		cur = self.connection.cursor()
		sql = "SELECT name FROM sqlite_master WHERE type='table'"
		cur.execute(sql)
		self.tables = set()
		for n in cur :
			self.tables.add(n[0])
	
	def __getattr__(self, name):
		return self.connection.__getattribute__(name)
		
	def tableExits(self, name) :
		return name in self.tables

	def dropTable(self, name) :
		sql = "DROP TABLE IF EXISTS %s" % name
		self.connection.cursor().execute(sql)
		self.connection.commit()
		
RabaConnection()

"""
class MasterTable(object) :
	"This class is a singleton and manages the MasterTable "
	__metaclass__ = Singleton

	def __init__(self, dbFileName = '/u/daoudat/usr/lib/python/rabaDB/rabaDB-0.db') :
		self.connection = sq.connect(dbFileName)

		if not self.tableExits('MasterTable') :
			print "table MasterTable not found, creating it..."
			sql = 'CREATE TABLE MasterTable (id INTEGER PRIMARY KEY AUTOINCREMENT);'
			self.connection.cursor().execute(sql)
			self.connection.commit()
	
		self.setTypes()
	
	def cursor(self) :
		return self.connection.cursor()
		
	def tableExits(self, name) :
		sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
		return self.connection.cursor().execute(sql, (name, )).fetchone() != None
	
	def setTypes(self) :
		self.types = set()
		cur = self.connection.cursor()
		cur.execute('PRAGMA table_info(relations);')
		for c in cur :
			self.types.add(c[1])
	
	def hasType(self, name) :
		return name in self.types

	def updateType(self, name, fields) :
		if not self.hasType(name) :
			sql = 'ALTER TABLE Relations ADD %s;' % name
			self.connection.cursor().execute(sql)
			self.connection.commit()
		
			self.connection.cursor().execute(sql)
			sql = 'CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, %s)' % (name, ', '.join(list(fields)))
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.types.add(name)
	
	def removeType(self, name) :
		if hasType(name) :
			self.connection.cursor().execute('UPDATE Relations SET %s=NULL WHERE 1;' % name)
			self.connection.cursor().execute('DROP TABLE IF EXISTS %s;' % name)
			self.connection.commit()
		
		self.types.remove(name)	
	
	def autoclean(self) :
		"TODO: Copies the Relations table into a new one removing all the collumns that have all their values to NULL
		and drop the tables that correspond to these tables"
		#TODO
		pass
"""
