import sqlite3 as sq
import os

class SingletonRabaConnection(type):
	_instances = {}
	def __call__(cls, *args, **kwargs):
		if cls not in cls._instances:
			cls._instances[cls] = super(SingletonRabaConnection, cls).__call__(*args, **kwargs)
		
		return cls._instances[cls]

class RabaConnection(object) :
	"""A class that manages the connection to the sqlite3 database. This a singleton, there can only be one single connection to a single file.
	Don't be afraid to call RabaConnection() as much as you want"""
	
	__metaclass__ = SingletonRabaConnection
	
	def __init__(self, dbFilename = None) :
		"The first time you instanciate a RabaConnection you must specify the dbFilename argument, the None default argument is just here so you can do RabaConnection() afterwards"
		if dbFilename == None :
			raise ValueError("The first time you instanciate a RabaConnection you must specify the dbFilename argument, the None default argument is just here so you can do RabaConnection() afterwards")
			
		self.connection = sq.connect(dbFilename)
		self.dbFilename = dbFilename
		
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
