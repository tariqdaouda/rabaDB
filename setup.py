import time
import sqlite3 as sq

#If set to True will print all sql commands and other goodies. Will also some checkings
_DEBUG_MODE = False

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
	"""A class that manages the connection to the sqlite3 database. Don't be afraid to call RabaConnection() as much as you want. By default Raba tries to be smart and commits only when
	you save a rabaobject but if you want complete controle over the commit process you can use setAutoCommit(False) and then use commit() manually"""

	__metaclass__ = RabaNameSpaceSingleton

	def __init__(self, namespace) :

		self.connection = sq.connect(RabaConfiguration(namespace).dbFile)
		self.namespace = namespace
		#self.setReadOnly(readOnly)
		self.loadedRabaClasses = {}
		self.saveIniator = None
		self.savedObject = set()
		self.inTransaction = False

		self.enableQueryPrint(True)
		self.enableStats(False)

		sql = "SELECT name, type FROM sqlite_master WHERE type='table'"
		cur = self.execute(sql)
		self.tables = set()
		#self.indexes = set()
		for n in cur :
			self.tables.add(n[0])
		#	elif n[1] == 'index' :
		#		self.indexes.add(n[0])

		#if not self.tableExits('rabalist_master') :
		#	sql = "CREATE TABLE rabalist_master (id INTEGER PRIMARY KEY AUTOINCREMENT, anchor_class NOT NULL, anchor_raba_id, relation_name NOT NULL, table_name NOT NULL, length DEFAULT 0)"
		#	self.execute(sql)
		#	self.connection.commit()
		#	self.tables.add('rabalist_master')

		if not self.tableExits('raba_tables_constraints') :
			sql = "CREATE TABLE raba_tables_constraints (table_name NOT NULL, constraints, PRIMARY KEY(table_name))"
			self.execute(sql)
			self.connection.commit()
			#self.tables.add('raba_tables_constraints')

		self.currentIds = {} #class name -> current max id

	def makeIndexTableName(self, table, field) :
		return "raba_%s_index_on_%s" %(table, field)

	def createIndex(self, table, field) :
		"Creates indexes for Raba Class a fields resulting in significantly faster SELECTs but potentially slower UPADTES/INSERTS and a bigger DBs"
		indexTable = self.makeIndexTableName(table, field)
		#if indexTable not in self.indexes :
		sql = "CREATE INDEX %s on %s(%s)" %(indexTable, table, field)
		try :
			self.execute(sql)
		except :
			return False
		return True
		#self.indexes.add(indexTable)

	def dropIndex(self, table, field) :
		"DROPs an index created by RabaDb see createIndexes()"
		indexTable = self.makeIndexTableName(table, field)
		sql = "DROP INDEX IF EXISTS %s" %(indexTable)
		self.execute(sql)
		#self.indexes.remove(indexTable)

	def eraseStats(self) :
		self.queryLogs = {'INSERT' : [], 'SELECT' : [], 'UPDATE' : [], 'DELETE' : [], 'DROP' : [], 'PRAGMA' : [],'CREATE' : []}
		self.queryCounts = {'INSERT' : 0, 'SELECT' : 0, 'UPDATE' : 0, 'DELETE' : 0, 'DROP' : 0, 'PRAGMA' : 0,'CREATE' : 0, 'TOTAL': 0}

	def enableStats(self, bol, logQueries = False) :
		self._enableStats = bol
		self._logQueries = logQueries
		if bol :
			self._enableStats = True
			self.eraseStats()
			self.startTime = time.time()

	def _logQuery(self, sql, values) :
		if self._enableStats :
			if sql[0].upper() == 'I' :
				k = 'INSERT'
			elif sql[0].upper() == 'S' :
				k = 'SELECT'
			elif sql[0].upper() == 'U' :
				k = 'UPDATE'
			elif sql[0].upper() == 'P' :
				k = 'PRAGMA'
			elif sql[0].upper() == 'C' :
				k = 'CREATE'
			elif sql[0].upper() == 'D' :
				if sql[1].upper() == 'E' :
					k = 'DELETE'
				elif sql[1].upper() == 'R' :
					k = 'DROP'
			else :
				kk = sql[:sql.find(' ')]
				if kk not in self.queryCounts[k] :
					self.queryCounts[k] = 0
					if self._logQueries :
						self.queryLogs[k] = []

			self.queryCounts[k] += 1
			self.queryCounts['TOTAL'] += 1
			if self._logQueries :
				vals = []
				for v in values :
					vals.append(repr(v))
				self.queryLogs[k].append((sql, vals))

	def enableQueryPrint(self, printQueries) :
		self._printQueries = printQueries

	def execute(self, sql, values = ()) :
		"executes an sql command for you or appends it to the current transacations. returns a cursor"
		sql = sql.strip()
		if self._printQueries : print sql, values
		cur = self.connection.cursor()
		cur.execute(sql, values)
		if self._enableStats :
			self._logQuery(sql, values)
		return cur

	def executemany(self, sql, values = [()]) :
		return self.executeMany(sql, values)

	def executeMany(self, sql, values = [()]) :
		sql = sql.strip()
		if self._printQueries : print sql, values
		cur = self.connection.cursor()
		cur.executemany(sql, values)
		if self._enableStats :
			self._logQuery(sql, values)
		return cur

	def printStats(self) :
		if self._enableStats :
			t = time.time() - self.startTime
			print "====Raba Connection %s stats====" % (self.namespace)
			if t < 60 :
				print "Been running for: %fsc" % t
			elif t < 3600 :
				print "Been running for: %fmin" % (t/60)
			else :
				print "Been running for: %fh" % (t/3600)

			print 'Query counts: '
			for k, v in self.queryCounts.iteritems() :
				print '\t', k
				print "\t\t raw counts:", v
				if self.queryCounts['TOTAL'] > 0 :
					print "\t\t ratio (total queries):", v/float(self.queryCounts['TOTAL'])
				else :
					print "\t\t ratio (total queries): 0/0"
				print "\t\t ratio (run time (sc)):", v/t
		else :
			print "====Raba Connection %s stats====> you must enable stats first" % (self.namespace)

	def beginTransaction(self) :
		"Raba commits at each save, unless you begin a transaction in wich cas everything will be commited when endTransaction() is called"
		self.inTransaction = True

	def endTransaction(self) :
		"commits the current transaction"
		self.connection.commit()
		self.inTransaction = False

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
		if self.saveIniator is obj and not self.inTransaction :
			self.saveIniator = None
			self.savedObject = set()
			self.connection.commit()
			return True
		return False

	def registerSave(self, obj) :
		"""Each object can only be save donce during a session, returns False if the object has already been saved. True otherwise"""
		if obj._runtimeId in self.savedObject :
			return False

		self.savedObject.add(obj._runtimeId)
		return True

	def delete(self, table, where, values = ()) :
		"""where is a string of condictions without the sql 'WHERE'. ex: deleteRabaObject('Gene', where = raba_id = ?, values = (33,))"""
		sql = 'DELETE FROM %s WHERE %s' % (table, where)
		return self.execute(sql, values)

	def getLastRabaId(self, cls) :
		"""keep track all loaded raba classes"""
		self.loadedRabaClasses[cls.__name__] = cls
		sql = 'SELECT MAX(raba_id) from %s LIMIT 1' % (cls.__name__)
		cur = self.execute(sql)
		res = cur.fetchone()
		try :
			return int(res[0])+1
		except TypeError:
			return  0

	def getNextRabaId(self, obj) :
		if obj.__class__.__name__ not in self.currentIds :
			self.currentIds[obj.__class__.__name__] = self.getLastRabaId(obj.__class__)

		self.currentIds[obj.__class__.__name__] += 1
		return self.currentIds[obj.__class__.__name__]

	def registerRabaClass(self, cls) :
		self.loadedRabaClasses[cls.__name__] = cls

	def getClass(self, name) :
		"""returns a loaded raba class given it's name"""
		try :
			return self.loadedRabaClasses[name]
		except KeyError :
			raise KeyError("There's not class named %s" % name)

	def tableExits(self, name) :
		return name in self.tables

	def dropTable(self, name) :
		sql = "DROP TABLE IF EXISTS %s" % name
		try :
			self.tables.remove(name)
		except KeyError :
			return None

		return self.execute(sql)

	def createTable(self, tableName, strFields) :
		'creates a table and resturns the ursor, if the table already exists returns None'
		if not self.tableExits(tableName) :
			sql = 'CREATE TABLE %s ( %s)' % (tableName, strFields)
			self.execute(sql)
			self.tables.add(tableName)
			return True
		return False

	def createRabaListTable(self, tableName) :
		'see createTable()'
		return self.createTable(tableName, 'raba_id INTEGER PRIMARY KEY AUTOINCREMENT, anchor_raba_id, value, type, obj_raba_class_name, obj_raba_id, obj_raba_namespace')

	def renameTable(self, old, new) :
		self.execute('ALTER TABLE %s RENAME TO %s' %(old, new))

	def dropColumnsFromRabaObjTable(self, name, lstFieldsToKeep) :
		"Removes columns from a RabaObj table. lstFieldsToKeep should not contain raba_id or json fileds"
		if len(lstFieldsToKeep) == 0 :
			raise ValueError("There are no fields to keep")

		cpy = name+'_copy'
		sqlFiledsStr = ', '.join(lstFieldsToKeep)
		self.createTable(cpy, 'raba_id INTEGER PRIMARY KEY AUTOINCREMENT, json, %s' % (sqlFiledsStr))
		sql = "INSERT INTO %s SELECT %s FROM %s;" % (cpy, 'raba_id, json, %s' % sqlFiledsStr, name)
		self.execute(sql)
		self.dropTable(name)
		self.renameTable(cpy, name)

	def getRabaObjectInfos(self, className, fieldsDct) :
		definedFields = []
		definedValues = []
		strWhere = ''
		for k, v in fieldsDct.items() :
			definedFields.append(k)
			definedValues.append(v)
			strWhere = '%s %s = ? AND' % (strWhere, k)

		strWhere = strWhere[:-4]
		if len(definedValues) > 0 :
			sql = 'SELECT * FROM %s WHERE %s' % (className, strWhere)
			return self.execute(sql, definedValues)

	#def registerRabalist(self, anchor_class_name, anchor_raba_id, relation_name) :
	#	table_name = self.makeRabaListTableName(anchor_class_name, relation_name)

	#	self.createTable(table_name, 'raba_id INTEGER PRIMARY KEY AUTOINCREMENT, anchor_raba_id, value, type, obj_raba_class_name, obj_raba_id, obj_raba_namespace')

	#	sql = 'INSERT INTO rabalist_master (anchor_class, anchor_raba_id, relation_name, table_name, length) VALUES (?, ?, ?, ?, ?)'
	#	cur = self.execute(sql, (anchor_class_name, anchor_raba_id, relation_name, table_name, 0))
	#	raba_id = cur.lastrowid
	#	return raba_id, str(table_name)

	#def unregisterRabalist(self, anchor_class_name, anchor_raba_id, relation_name) :
	#	table_name = self.makeRabaListTableName(anchor_class_name, relation_name)
	#	sql = 'DELETE FROM rabalist_master WHERE table_name = ? and anchor_raba_id = ?'
	#	self.execute(sql, (table_name, anchor_raba_id))

	def dropRabalist(self, anchor_class_name, relation_name) :
		table_name = self.makeRabaListTableName(anchor_class_name, relation_name)
		self.dropTable(table_name)

	#	sql = 'DELETE FROM rabalist_master WHERE table_name = ?'
	#	self.execute(sql, (table_name, ))

	def makeRabaListTableName(self, anchor_class_name, relation_name) :
		return 'RabaList_%s_for_%s' % (relation_name, anchor_class_name)

	#def updateRabaListLength(self, raba_id, newLength) :
	#	sql = "UPDATE rabalist_master SET length = ? WHERE id = ?"
	#	self.execute(sql, (newLength, raba_id))

	#def getRabaListInfos(self, **fields) :
	#	if 'raba_id' in fields :
	#		sql = 'SELECT * FROM rabalist_master WHERE id = ?'
	#		cur = self.execute(sql, (fields['raba_id'], ))
	#	elif 'anchor_class_name' in fields and 'relation_name' in fields and 'anchor_raba_id' in fields :
	#		sql = 'SELECT * FROM rabalist_master WHERE table_name = ? and anchor_raba_id = ?'
	#		cur = self.execute(sql, (self.makeRabaListTableName(fields['anchor_class_name'], fields['relation_name']), fields['anchor_raba_id']))
	#	else :
	#		return None

	#	res = cur.fetchone()
	#	if res == None :
	#		return None

	#	res2 = cur.fetchone()
	#	if res2 != None :
	#		raise ValueError("The parameters %s are valid for more than one element" % fields)

	#	return {'raba_id' : res[0], 'anchor_class' : str(res[1]), 'anchor_raba_id' : str(res[2]), 'relation_name' : str(res[3]), 'table_name' : str(res[4]), 'length' : int(res[5])}

	#def getRabaListTables(self) :
	#	sql = 'SELECT * FROM rabalist_master'
	#	cur = self.execute.execute(sql)

	#	res = []
	#	for c in cur :
	#		res.append(c[0])

	#	return res

	def commit(self) :
		"""Only commits it not in a transaction"""
		if not self.inTransaction :
			self.connection.commit()

	def forceCommit(self) :
		"""Forces a commit"""
		self.connection.commit()

	def __getattr__(self, name):
		return self.connection.__getattribute__(name)
