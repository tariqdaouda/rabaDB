import sqlite3 as sq

class RabaNameSpaceSingleton(type):
	_instances = {}
	
	def __call__(cls, *args, **kwargs):
		if len(args) < 1 :
			raise ValueError('The first argument to %s must be a namespace' % cls.__name__)
		
		if 'namespace' in kwargs :
			nm = kwargs['namespace']
		else :
			nm = args[0]
		key = (cls.__name__, nm)
		
		if key not in cls._instances:
			cls._instances[key] = type.__call__(cls, *args, **kwargs)
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
	
	
class RabaConnection(object) :
	"""A class that manages the connection to the sqlite3 database. Don't be afraid to call RabaConnection() as much as you want"""
	
	__metaclass__ = RabaNameSpaceSingleton
	
	def __init__(self, namespace) :
		self.connection = sq.connect(RabaConfiguration(namespace).dbFile)
		#self.setReadOnly(readOnly)
		
		cur = self.connection.cursor()
		sql = "SELECT name FROM sqlite_master WHERE type='table'"
		cur.execute(sql)
		self.tables = set()
		for n in cur :
			self.tables.add(n[0])
		
		if not self.tableExits('rabalist_master') :
			sql = "CREATE TABLE rabalist_master (id INTEGER PRIMARY KEY AUTOINCREMENT, anchor_class NOT NULL, anchor_raba_id, relation_name NOT NULL, table_name NOT NULL, length DEFAULT 0)"
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add('rabalist_master')
		
		if not self.tableExits('raba_tables_constraints') :
			sql = "CREATE TABLE raba_tables_constraints (table_name NOT NULL, constraints, PRIMARY KEY(table_name))"
			self.connection.cursor().execute(sql)
			self.connection.commit()
			self.tables.add('raba_tables_constraints')
	
		self.loadedRabaClasses = {}
		self.saveIniator = None
		self.savedObject = set()
	
	def initateSave(self, obj) :
		"""Tries to initiates a save sessions. Each object can only be saved once during a session.
		The session begins when a raba object initates it and ends when this object and all it's dependencies have been saved"""
		if self.saveIniator != None :
			return False
		self.saveIniator = obj
		return True
	
	def freeSave(self, obj) :
		"""THIS IS WHERE COMMITS TAKE PLACE!
		Ends a saving session, only the initiator can end a session. The commit is performed at the end of the session"""
		if self.saveIniator is obj :
			self.saveIniator = None
			self.savedObject = set()
			self.connection.commit()
			return True
		return False
	
	def canISave(self, obj) :
		"""Each object can only be save donce during a session, returns False if the object has already been saved. True otherwise"""
		if obj._runtimeId in self.savedObject :
			return False
		
		self.savedObject.add(obj._runtimeId)
		return True
	
	def registerRabaClass(self, cls) :
		"""keep track all loaded raba classes"""
		self.loadedRabaClasses[cls.__name__] = cls
	
	def getClass(self, name) :
		"""returns a loaded raba class given it's name"""
		return self.loadedRabaClasses[name]
	
	def tableExits(self, name) :
		return name in self.tables

	def dropTable(self, name) :
		if self.tableExits(name) :
			sql = "DROP TABLE IF EXISTS %s" % name
			self.connection.cursor().execute(sql)
			#self.connection.commit()
			self.tables.remove(name)
		
	def createTable(self, tableName, strFields) :
		if not self.tableExits(tableName) :
			sql = 'CREATE TABLE %s ( %s)' % (tableName, strFields)
			#print sql
			self.connection.cursor().execute(sql)
			#self.connection.commit()
			self.tables.add(tableName)
	
	def getRabaObjectInfos(self, className, fieldsDct) :
		definedFields = []
		definedValues = []
		strWhere = ''
		for k, v in fieldsDct.items() :
			definedFields.append(k)
			definedValues.append(v)
			strWhere = '%s %s = ? AND' % (strWhere, k)
			
		strWhere = strWhere[:-4]
		cur = self.connection.cursor()
		if len(definedValues) > 0 :
			sql = 'SELECT * FROM %s WHERE %s' % (className, strWhere)
			#print sql, definedValues
			cur.execute(sql, definedValues)
		return cur
	
	def registerRabalist(self, anchor_class_name, anchor_raba_id, relation_name) :
		table_name = self.makeRabaListTableName(anchor_class_name, relation_name)
		
		self.createTable(table_name, 'raba_id INTEGER PRIMARY KEY AUTOINCREMENT, anchor_raba_id, value_or_raba_id, type')
		
		sql = 'INSERT INTO rabalist_master (anchor_class, anchor_raba_id, relation_name, table_name, length) VALUES (?, ?, ?, ?, ?)'
		cur = self.connection.cursor()
		cur.execute(sql, (anchor_class_name, anchor_raba_id, relation_name, table_name, 0))
		raba_id = cur.lastrowid
		#self.connection.commit()
		return raba_id, str(table_name)
	
	def unregisterRabalist(self, anchor_class_name, anchor_raba_id, relation_name) :
		table_name = self.makeRabaListTableName(anchor_class_name, relation_name)
		
		sql = 'DELETE FROM rabalist_master WHERE table_name = ? and anchor_raba_id = ?'
		cur = self.connection.cursor()
		cur.execute(sql, (table_name, anchor_raba_id))
		
	def dropRabalist(self, anchor_class_name, relation_name) :
		table_name = self.makeRabaListTableName(anchor_class_name, relation_name)
		self.dropTable(table_name)
		
		sql = 'DELETE FROM rabalist_master WHERE table_name = ?' 
		cur = self.connection.cursor()
		cur.execute(sql, (table_name, ))
		#self.connection.commit()
	
	def makeRabaListTableName(self, anchor_class_name, relation_name) :
		return 'RabaList_%s_for_%s' % (relation_name, anchor_class_name)
		
	def updateRabaListLength(self, raba_id, newLength) :
		sql = "UPDATE rabalist_master SET length = ? WHERE id = ?"
		self.connection.cursor().execute(sql, (newLength, raba_id))
		#self.connection.commit()

	def getRabaListInfos(self, **fields) :
		try :
			sql = 'SELECT * FROM rabalist_master WHERE id = ?'
			cur = self.connection.cursor()
			cur.execute(sql, (fields['raba_id'], ))
		except KeyError :
			try :
				sql = 'SELECT * FROM rabalist_master WHERE table_name = ? and anchor_raba_id = ?'
				cur = self.connection.cursor()
				cur.execute(sql, (self.makeRabaListTableName(fields['anchor_class_name'], fields['relation_name']), fields['anchor_raba_id']))
			except KeyError :
				return None
			
		res = cur.fetchone()
		if res == None :
			return None
		
		res2 = cur.fetchone()
		if res2 != None :
			raise ValueError("The parameters %s are valid for more than one element" % fields)
		
		return {'raba_id' : res[0], 'anchor_class' : str(res[1]), 'anchor_raba_id' : str(res[2]), 'relation_name' : str(res[3]), 'table_name' : str(res[4]), 'length' : int(res[5])}
	
	def getRabaListTables(self) :
		sql = 'SELECT * FROM rabalist_master'
		cur = self.connection.cursor()
		cur.execute(sql)
		
		res = []
		for c in cur :
			res.append(c[0])
		
		return res

	def commit(self) :
		"""Forces a commit"""
		self.connection.commit()
		
	def __getattr__(self, name):
		return self.connection.__getattribute__(name)
