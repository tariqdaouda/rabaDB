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
