import sqlite3 as sq
import os

class confParser :
	def __init__(self, fp) :
		self.fp = fp
		try :
			f = open(fp)
			self.lines = f.readlines()
			f.close()
		except IOError:
			self.createFile()
			raise IOError("Can't find rabaDB.conf file in current directory, i've created one for you. Please fill it an rerun the script")
		
		self.parse()
	
	def createFile(self) :
		pattern = """#This is a comment
#enter the filename to the rabaDB file to be used (it it dosen't exist an empty DB will be created)
database filepath:"""
		
		f = open('rabaDB.conf', 'w')
		f.write(pattern)
		f.close()
		
	def parse(self) :
		
		self.values = {}
		for l in self.lines :
			if l[0] != '#' :
				sl = l.split(':')
				if len(sl) > 2 :
					raise ValueError("line %s of %s must contain only one ':'" % (l, self.fp))
			
				if sl[0].strip() == 'database filepath' :
					self.values[sl[0]] = sl[1].strip()
		
		if 'database filepath' not in self.values  or self.values['database filepath'] == '' :
			raise ValueError("the field 'database filepath' of %s must be filled" % (self.fp))
			
	def __getitem__(self, i) :
		return self.values[i]

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
	
	def __init__(self) :
		conf = confParser('rabaDB.conf')
		
		self.connection = sq.connect(conf['database filepath'])
		
		cur = self.connection.cursor()
		sql = "SELECT name FROM sqlite_master WHERE type='table'"
		cur.execute(sql)
		self.tables = set()
		for n in cur :
			self.tables.add(n[0])
		
		if not self.tableExits('rabalist_master') :
			sql = "CREATE TABLE rabalist_master (anchor_type, relation_name, elements_type NOT NULL, table_name NOT NULL, PRIMARY KEY(anchor_type, relation_name))"
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add('rabalist_master')
			
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
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add(tableName)
			
	def registerRabalist(self, anchor_type, relation_name, elements_type, table_name) :
		sql = 'INSERT INTO rabalist_master (anchor_type, relation_name, elements_type, table_name) VALUES (?, ?, ?, ?)'
		self.connection.cursor().execute(sql, (anchor_type.__name__, relation_name, elements_type.__name__, table_name))
		self.connection.commit()
	
	def unregisterRabaList(self, anchor_type, relation_name) :
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
