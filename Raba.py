import sqlite3 as sq
import os, copy, types, cPickle, random, json, abc, sys#, weakref
from collections import MutableSequence

from setup import RabaConnection, RabaConfiguration
import fields as RabaFields

def makeRabaObjectSingletonKey(clsName, namespace, raba_id) :
	return (clsName, namespace, raba_id)

def _recClassCheck(v, cls) :
	if v is cls : return True

	res = False
	for a in v.__bases__ :
		if a is cls :
			return True
		res = res or _recClassCheck(a, cls)
	return res

def isRabaClass(v) :
	return _recClassCheck(v, Raba)

def isRabaList(v) :
	return _recClassCheck(v.__class__, RabaList) or isRabaListPupa(v)

def isRabaListPupa(v) :
	return _recClassCheck(v.__class__, RabaListPupa)

def isRabaObject(v) :
	return isRabaClass(v.__class__) or isRabaObjectPupa(v)

def isRabaObjectPupa(v) :
	return _recClassCheck(v.__class__, RabaPupa)

def isPythonPrimitive(v) :
	primTypes = [types.IntType, types.LongType, types.FloatType, types.StringType, types.UnicodeType, types.BufferType, types.NoneType]
	for t in primTypes :
		if isinstance(v, t) :
			return True
	return False

class _RabaPupaSingleton_Metaclass(type):
	_instances = {}
	def __call__(clsObj, *args, **kwargs):

		if 'classObj' in kwargs :
			cls = kwargs['classObj']
		else :
			cls = args[0]

		if 'raba_id' in kwargs :
			raba_id = kwargs['raba_id']
		else :
			raba_id = args[1]

		key = makeRabaObjectSingletonKey(cls.__name__, cls._raba_namespace, raba_id)
		if key in _RabaSingleton_MetaClass._instances :
			return _RabaSingleton_MetaClass._instances[key]
		elif key not in clsObj._instances :
			clsObj._instances[key] = super(_RabaPupaSingleton_Metaclass, clsObj).__call__(*args, **kwargs)

		return clsObj._instances[key]


class RabaListPupaSingleton_Metaclass(abc.ABCMeta):
	_instances = {}

	def __call__(clsObj, *args, **kwargs):

		anchorObj = kwargs['anchorObj']
		relationName = kwargs['relationName']

		key = (anchorObj._runtimeId, relationName)

		if key in RabaListSingleton_Metaclass._instances :
			return RabaListSingleton_Metaclass._instances[key]
		if key in clsObj._instances :
			return clsObj._instances[key]

		connection = RabaConnection(anchorObj._raba_namespace)

		infos = connection.getRabaListInfos(anchor_class_name = anchorObj._rabaClass.__name__, anchor_raba_id = anchorObj.raba_id, relation_name = relationName)
		if infos == None :
			infos = {}
			infos['raba_id'] = None
			infos['table_name'] = None
			infos['length'] = 0

		for k in kwargs :
			infos[k] = kwargs[k]

		obj = super(RabaListPupaSingleton_Metaclass, clsObj).__call__(*args, **infos)

		clsObj._instances[key] = obj
		return obj

class RabaListSingleton_Metaclass(abc.ABCMeta):
	_instances = {}

	def __call__(clsObj, *args, **kwargs):

		if 'anchorObj' in kwargs and 'relationName' in kwargs :
			anchorObj = kwargs['anchorObj']
			relationName = kwargs['relationName']

			key = (anchorObj._runtimeId, relationName)

			if key in clsObj._instances :
				return clsObj._instances[key]

			clsObj._instances[key] = super(RabaList_Metaclass, clsObj).__call__(*args, **kwargs)

			return clsObj._instances[key]
		return super(RabaListSingleton_Metaclass, clsObj).__call__(*args, **kwargs)

class _RabaSingleton_MetaClass(type) :

	_instances = {}

	def __new__(cls, name, bases, dct) :
		if name != 'Raba' :
			fields = []
			columns = {'raba_id' : 0, 'json' : 1}
			columnsToLowerCase = {'raba_id' : 'raba_id', 'json' : 'json'}

			i = len(columns)
			for k, v in dct.items():
				sk = str(k)
				if RabaFields.isField(v) :
					fields.append(sk)
					columns[sk.lower()] = i
					columnsToLowerCase[sk.lower()] = sk
					i += 1
			try :
				con = RabaConnection(dct['_raba_namespace'])
			except KeyError :
				raise ValueError("The class %s has no defined namespace, please add a valid '_raba_namespace' to class attributes" % name)

			uniqueStr = ''
			if '_raba_uniques' in dct :
				for c in dct['_raba_uniques'] :
					uniqueStr += 'UNIQUE %s ON CONFLICT REPLACE, ' % str(c)

			uniqueStr = uniqueStr[:-2]

			if not con.tableExits(name) :
				idJsonStr = 'raba_id INTEGER PRIMARY KEY, json '
				if len(fields) > 0 :
					if len(uniqueStr) > 0 :
						con.createTable(name, '%s, %s, %s' % (idJsonStr, ', '.join(list(fields)), uniqueStr))
					else :
						con.createTable(name, '%s, %s' % (idJsonStr, ', '.join(list(fields))))
				else :
					con.createTable(name, '%s' % idJsonStr)

				sqlCons = 'INSERT INTO raba_tables_constraints (table_name, constraints) VALUES (?, ?)'
				con.execute(sqlCons, (name, uniqueStr))
				#con.commit()
			else :
				sql = 'SELECT constraints FROM raba_tables_constraints WHERE table_name = ?'

				cur = con.execute(sql, (name,))
				res = cur.fetchone()

				if res != None and res[0]!= '' and res[0] != uniqueStr :
					sys.stderr.write('Warning: The unique contraints have changed from:\n\t%s\n\nto:\n\t%s.\n-Unique constraints modification is not supported yet-\n' %(res[0], uniqueStr))
					#raise FutureWarning('Warning: The unique contraints have changed from:\n\t%s\n\nto:\n\t%s.\n-Unique constraints modification is not supported yet-\n' %(res[0], uniqueStr))

				cur = con.execute('PRAGMA table_info("%s")' % name)
				tableColumns = set()
				fieldsToKill = []

				for c in cur :
					if c[1] != 'raba_id' and c[1] != 'json' and c[1].lower() not in columns :
						#print c[1].lower(), columns
						#Destroy field that have mysteriously desapeared
						fieldsToKill.append('%s = NULL' % c[1])
						con.dropRabalist(name, c[1])
					#else :
					#	columns[c[1]] = c[0]

					tableColumns.add(c[1].lower())

				if len(fieldsToKill) > 0 :
					sql = 'UPDATE %s SET %s WHERE 1;' % (name , ', '.join(fieldsToKill))
					if _DEBUG_MODE : print sql
					cur.execute(sql)

				for k in columns :
					if k.lower() not in tableColumns :
						cur.execute('ALTER TABLE %s ADD COLUMN %s' % (name, k))

			#columns['raba_id'] = 0
			#columns['json'] = 1
			dct['raba_id'] = RabaFields.Primitive()
			dct['json'] = RabaFields.Primitive()
			dct['columns'] = columns
			dct['columnsToLowerCase'] = columnsToLowerCase

			clsObj = type.__new__(cls, name, bases, dct)
			con.registerRabaClass(clsObj)
			return clsObj

		return type.__new__(cls, name, bases, dct)

	def __call__(cls, **fieldsDct) :
		if cls == Raba :
			return super(_RabaSingleton_MetaClass, cls).__call__(**fieldsDct)

		if 'raba_id' in fieldsDct :
			key = makeRabaObjectSingletonKey(cls.__name__, cls._raba_namespace, fieldsDct['raba_id'])
			if key in cls._instances :
				return cls._instances[key]
		else :
			key = None

		params = copy.copy(fieldsDct)
		for p, v in params.items() :
			if p in cls.columns :
				if isRabaObject(v) :
					params[p] = v.getJsonEncoding()
			else :
				del(params[p])

		connection = RabaConnection(cls._raba_namespace)
		ret = connection.getRabaObjectInfos(cls.__name__, params)

		if ret != None :
			dbLine = ret.fetchone()
		else :
			dbLine = None

		if dbLine != None :
			if ret.fetchone() != None :
				raise KeyError("More than one object fit the arguments you've prodided to the constructor")

			if 'raba_id' in fieldsDct and res == None :
				raise KeyError("There's no %s with a raba_id = %s" %(self._rabaClass.__name__, fieldsDct['raba_id']))

			raba_id = dbLine[cls.columns['raba_id']]
			key = makeRabaObjectSingletonKey(cls.__name__, cls._raba_namespace, raba_id)
			if key in cls._instances :
				return cls._instances[key]

			obj = type.__call__(cls)
			obj._raba__init__(initDbLine = dbLine, **fieldsDct)
			cls._instances[key] = obj

		elif len(params) > 0 :
			raise KeyError("Couldn't find any object that fit the arguments you've prodided to the constructor")
		else :
			obj = type.__call__(cls, **fieldsDct)
			obj._raba__init__(**fieldsDct)

		return obj

class RabaPupa(object) :
	"""One of the founding principles of RabaDB is to separate the storage from the code. Fields are stored in the DB while the processing only depends
	on your python code. This approach ensures a higher degree of stability by preventing old objects from lurking inside the DB before popping out of nowhere several decades afterwards.
	According to this apparoach, raba objects are not serialised but transformed into pupas before being stored. A pupa is a very light object that contains only a reference
	to the raba object class, and it's unique raba_id. Upon asking for one of the attributes of a pupa, it magically transforms into a full fledged raba object. This process is completly transparent to the user. Pupas also have the advantage of being light weight and also ensure that the only raba objects loaded are those explicitely accessed, thus potentialy saving a lot of memory.
	For a pupa self._rabaClass refers to the class of the object "inside" the pupa.
	"""

	__metaclass__ = _RabaPupaSingleton_Metaclass

	def __init__(self, classObj, raba_id) :
		self._rabaClass = classObj
		self.raba_id = raba_id
		self._raba_namespace = classObj._raba_namespace
		self.__doc__ = classObj.__doc__

	def develop(self) :
		def getAttr(name) :
			return object.__getattribute__(self, name)

		def setAttr(name, value) :
			object.__setattr__(self, name, value)

		rabaClass = getAttr('_rabaClass')
		uniqueId = getAttr('raba_id')
		connection = RabaConnection(getAttr('_raba_namespace'))
		dbLine = connection.getRabaObjectInfos(getAttr('_rabaClass').__name__, {'raba_id' : uniqueId}).fetchone()
		setAttr('__class__', rabaClass)
		purge = getAttr('__dict__').keys()
		for k in purge :
			delattr(self, k)

		self._rabaClass = rabaClass
		self.connection = connection
		rabaClass._raba__init__(self, initDbLine = dbLine)
		rabaClass.__init__(self)

	def getDctDescription(self) :
		"returns a dict describing the object"
		return  {'type' : RabaFields.RABA_FIELD_TYPE_IS_RABA_OBJECT, 'className' : self._rabaClass.__name__, 'raba_id' : self.raba_id, 'raba_namespace' : self._raba_namespace}

	def getJsonEncoding(self) :
		"returns a json encoding of self.getDctDescription()"
		return json.dumps(self.getDctDescription())

	def __getattr__(self, name) :
		develop = object.__getattribute__(self, "develop")
		develop()

		return self.__getattribute__(name)

	def __str__(self) :
		self.develop()
		return str(self)

	def __repr__(self) :
		return "<RabaObj pupa: %s, raba_id %s>" % (self._rabaClass.__name__, self.raba_id)

class Raba(object):
	"All raba object inherit from this class"
	__metaclass__ = _RabaSingleton_MetaClass

	def __init__(self, *a, **b) :
		pass

	def _initDbLine(self, dbLine) :
		self.raba_id = dbLine[self.__class__.columns['raba_id']]
		self.json = dbLine[self.__class__.columns['json']]

		lists = []
		for kk, i in self.columns.items() :
			k = self.columnsToLowerCase[kk.lower()]
			elmt = getattr(self._rabaClass, k)
			if RabaFields.isPrimitiveField(elmt) :
				try :
					self.__setattr__(k, cPickle.loads(str(dbLine[i])))
				except :
					self.__setattr__(k, dbLine[i])

			elif RabaFields.isRabaObjectField(elmt) :
				if dbLine[i] != None :
					val = json.loads(dbLine[i])
					objClass = RabaConnection(val["raba_namespace"]).getClass(val["className"])
					self.__setattr__(k, RabaPupa(objClass, val["raba_id"]))
			elif RabaFields.isRabaListField(elmt) :
				lists.append(k)
			else :
				raise ValueError("Unable to set field %s to %s in Raba object %s" %(k, dbLine[i], self._rabaClass.__name__))

		for k in lists :
			rlp = RabaListPupa(anchorObj = self, relationName = k)
			self.__setattr__(k, rlp)

	def _raba__init__(self, **fieldsSet) :
		self.sqlSave = {}
		self.sqlSaveQMarks = {}
		self.listsToSave = {}

		if self.__class__ is Raba :
			raise TypeError('Raba class should never be instanciated, use inheritance')

		self._runtimeId = (self.__class__.__name__, random.random()) #this is used only during runtime ex, to avoid circular calls
		self._rabaClass = self.__class__

		self.connection = RabaConnection(self._rabaClass._raba_namespace)
		self.rabaConfiguration =  RabaConfiguration(self._rabaClass._raba_namespace)

		self._saved = False #True if present in the database

		if 'initDbLine' in fieldsSet and 'initDbLine' != None :
			self._initDbLine(fieldsSet['initDbLine'])
			self._saved = True

		if self.raba_id == None :
			self.raba_id = self.connection.getRabaId(self)

	def autoclean(self) :
		"""TODO: Copies the table into a new one droping all the collumns that have all their values to NULL
		and drop the tables that correspond to these tables"""
		raise FutureWarning("sqlite does not support column droping, work aroun not implemented yet")

	def pupa(self) :
		"""returns a pupa version of self"""
		return RabaPupa(self.__class__, self.raba_id)

	def develop(self) :
		"Dummy fct, so when you call develop on a full developed object you don't get nasty exceptions"
		pass

	@classmethod
	def addIndex(cls, field) :
		con = RabaConnection(cls._raba_namespace)
		if RabaFields.isRabaListField(object.__getattribute__(cls, field)) :
			name = con.makeRabaListTableName(cls.__name__, field)
			ffield = 'anchor_raba_id'
		else :
			name = cls.__name__
			ffield = field

		con.createIndex(name, ffield)

	@classmethod
	def removeIndex(cls, field) :
		con = RabaConnection(cls._raba_namespace)
		if RabaFields.isRabaListField(object.__getattribute__(cls, field)) :
			name = con.makeRabaListTableName(cls.__name__, field)
			ffield = 'anchor_raba_id'
		else :
			name = cls.__name__
			ffield = field

		con.dropIndex(name, ffield)

	def mutated(self) :
		'returns True if the object has changed since the last save'
		return len(self.sqlSave) > 0

	def save(self) :
		if self.mutated() :
			for k, v in self.listsToSave.iteritems() :
				v._save()
				self.sqlSave[k] = len(v)
				if not self._saved : #this dict is only for optimisation purpose for generating the insert sql
					self.sqlSaveQMarks[k] = '?'

			self.sqlSave['json'] = self.getJsonEncoding()
			if not self._saved : #this dict is only for optimisation purpose for generating the insert sql
				self.sqlSaveQMarks['json'] = '?'

			if not self._saved :
				sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ', '.join(self.sqlSave.keys()), ', '.join(self.sqlSaveQMarks.values()))
			else :
				sql = 'UPDATE %s SET %s = ? WHERE raba_id = ?' % (self.__class__.__name__, ' = ?, '.join(self.sqlSave.keys()))

			self.connection.execute(sql, self.sqlSave.values())

			self._saved = True
			self.sqlSave = {}
			self.sqlSaveQMarks = {}
			self.listsToSave = {}

	def delete(self) :
		self.connection.deleteRabaObject(self)

	def copy(self) :
		v = copy.copy(self)
		v.raba_id = None
		return v

	def getDctDescription(self) :
		"returns a dict describing the object"
		return  {'type' : RabaFields.RABA_FIELD_TYPE_IS_RABA_OBJECT, 'className' : self._rabaClass.__name__, 'raba_id' : self.raba_id, 'raba_namespace' : self._raba_namespace}

	def getJsonEncoding(self) :
		"returns a json encoding of self.getDctDescription()"
		return json.dumps(self.getDctDescription())

	def set(self, **args) :
		"set multiple values quickly, ex : name = woopy"
		for k, v in args.items() :
			setattr(self, k, v)

	def __setattr__(self, k, v) :
		vv = v
		if hasattr(self.__class__, k) and RabaFields.isField(getattr(self.__class__, k)) :
			vSQL = None
			if not RabaFields.isRabaListField(getattr(self.__class__, k)) :
				classType = getattr(self.__class__, k)
				if not classType.check(vv) :
					raise ValueError("Unable to set '%s' to value '%s'. Constrain function violation" % (k, vv))
				if isRabaObject(vv) :
					vSQL = vv.getJsonEncoding()
				elif isPythonPrimitive(vv):
					vSQL = vv
				else :
					vSQL = buffer(cPickle.dumps(vv))

				self.sqlSave[k] = vSQL
				if not self._saved : #this dict is only for optimisation purpose for generating the insert sql
					self.sqlSaveQMarks[k] = '?'
			else :
				if not isRabaList(vv) and not isRabaListPupa(vv) :
					try :
						vv = RabaList(v)
					except :
						raise ValueError("Unable to set '%s' to value '%s'. Value is not a valid RabaList" % (k, vv))

				currList = getattr(self, k)
				if vv is not currList and len(currList) > 0 :
					currList.erase()
					self.connection.unregisterRabalist(anchor_class_name = self.__class__.__name__, anchor_raba_id = self.raba_id, relation_name = k)

				vv._attachToObject(self, k)
				self.listsToSave[k] = vv #self.sqlSave[k] and self.sqlSaveQMarks[k] are updated in self.save() for lists

		object.__setattr__(self, k, vv)

	def __getattribute__(self, k) :
		try :
			elmt = object.__getattribute__(self, k)
			if RabaFields.isRabaListField(elmt) : #if empty
				elmt = RabaListPupa(anchorObj = self, relationName = k)
				object.__setattr__(self, k, elmt)
			elif RabaFields.isField(elmt) :
				elmt = elmt.default

			return elmt
		except AttributeError :
			return self.__getattr__(k)

	def __getattr__(self, k) :
		raise AttributeError('%s has no attribute "%s"' %(repr(self), k))

	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem__(self, k, v) :
		self. __setattr__(k, v)

	def __repr__(self) :
		return "<Raba obj: %s, raba_id: %s>" % (self._runtimeId, self.raba_id)

	def help(self) :
		"returns a string of parameters"
		fil = []
		for k, v in self.__class__.__dict__.items() :
			if RabaFields.isField(v) :
				fil.append(k)

		return '\n==========\nAvailable fields for %s:\n--\n%s\n=======\n' %(self.__class__.__name__, ', '.join(fil))

class RabaListPupa(MutableSequence) :

	_isRabaList = True
	__metaclass__ = RabaListPupaSingleton_Metaclass

	def __init__(self, **kwargs) :
		self._runtimeId = (self.__class__.__name__, random.random()) #this is using only during runtime ex, to avoid circular calls
		self.raba_id = kwargs['raba_id']
		self.tableName = kwargs['table_name']
		self.length = kwargs['length']
		self.anchorObj = kwargs['anchorObj']
		self.relationName = kwargs['relationName']
		self._raba_namespace = self.anchorObj._raba_namespace
		self.connection = RabaConnection(self._raba_namespace)

		self.tableName = self.connection.makeRabaListTableName(self.anchorObj._rabaClass.__name__, self.relationName)

	def develop(self) :
		MutableSequence.__setattr__(self, '__class__', RabaList)

		initFromPupa = {}

		initFromPupa['relationName'] = MutableSequence.__getattribute__(self, 'relationName')
		initFromPupa['anchorObj'] = MutableSequence.__getattribute__(self, 'anchorObj')
		initFromPupa['_raba_namespace'] =  MutableSequence.__getattribute__(self, '_raba_namespace')
		initFromPupa['tableName'] = MutableSequence.__getattribute__(self, 'tableName')
		initFromPupa['raba_id'] = MutableSequence.__getattribute__(self, 'raba_id')

		purge = MutableSequence.__getattribute__(self, '__dict__').keys()
		for k in purge :
			delattr(self, k)

		RabaList.__init__(self, initFromPupa = initFromPupa)
		RabaList._attachToObject(self, initFromPupa['anchorObj'], initFromPupa['relationName'])

	def __getitem__(self, i) :
		self.develop()
		return self[i]

	def __delitem__(self, i) :
		self.develop()
		self.__delitem__(i)

	def __setitem__(self, k, v) :
		self.develop()
		self.__setitem__(k, v)

	def insert(self, k, i, v) :
		self.develop()
		self.insert(i, v)

	def append(self, k) :
		self.develop()
		self.append(k)

	def _attachToObject(self, anchorObj, relationName) :
		"dummy fct for compatibility reasons, a RabaListPupa is attached by default"
		#MutableSequence.__getattribute__(self, "develop")()
		self.develop()
		self._attachToObject(anchorObj, relationName)

	def _save(self, *args, **kwargs) :
		"dummy fct for compatibility reasons, a RabaListPupa represents an unmodified list so there's nothing to save"
		self.develop()
		RabaList._save(self)

	def __getattr__(self, name) :
		self.develop()
		return MutableSequence.__getattribute__(self, name)

	def __repr__(self) :
		return "[%s length: %d, relationName: %s, anchorObj: %s, raba_id: %s]" % (self._runtimeId, self.length, self.relationName, self.anchorObj, self.raba_id)

	def __len__(self) :
		return self.length

class RabaList(MutableSequence) :
	"""A RabaList is a list that can only contain Raba objects of the same class or (Pupas of the same class). They represent one to many relations and are stored in separate
	tables that contain only one single line"""

	_isRabaList = True
	__metaclass__ = RabaListSingleton_Metaclass

	def _checkElmt(self, v) :
		if self.anchorObj != None :
			return getattr(self.anchorObj._rabaClass, self.relationName).check(v)
		else :
			return True

	def _checkSelf(self) :
		"""Checks the entire list, returns (faultyElmt, list namespace), if the list passes the check, faultyElmt = None"""
		for e in self :
			if not self._checkElmt(e) :
				return e
		return None

	def _dieInvalidRaba(self, v) :
		st = """The element %s can't be added to the list, possible causes:
		-The element is a RabaObject wich namespace is different from list's namespace
		-The element violates the constraint function""" % v

		raise TypeError(st)

	def __init__(self, *listElements, **listArguments) :
		"""To avoid the check of all elements of listElements during initialisation pass : noInitCheck = True as argument
		It is also possible to define both the anchor object and the namespace durint initalisation using argument keywords: anchorObj and namespace. But only do it
		if you really now what you are doing."""
		self._runtimeId = (self.__class__.__name__, random.random())#this is used only during runtime ex, to avoid circular calls

		self.raba_id = None
		self.relationName = None
		self.tableName = None
		self.anchorObj = None

		self._raba_namespace = None
		self.connection = None
		self.rabaConfiguration = None
		self._saved = False
		self._mutated = True

		if len(listElements) > 0 :
			self.data = list(listElements[0])
		else :
			self.data = []

		if 'initFromPupa' in listArguments :
			pupaInit = listArguments['initFromPupa']
			self.anchorObj = pupaInit['anchorObj']
			self.relationName = pupaInit['relationName']
			self._raba_namespace = self.anchorObj._raba_namespace
			self.raba_id = pupaInit['raba_id']
			self.tableName = pupaInit['tableName']

			self._setNamespaceConAndConf(self.anchorObj._raba_namespace)

			if self.raba_id == None :
				self.raba_id, self.tableName = self.connection.registerRabalist(self.anchorObj._rabaClass.__name__, self.anchorObj.raba_id, self.relationName)

			sql, values = 'SELECT * FROM %s WHERE anchor_raba_id = ?' % self.tableName, (self.anchorObj.raba_id, )
			cur = self.connection.execute(sql, values)
			for aidi in cur :
				self._saved = True
				value = aidi[2]
				typ = aidi[3]
				className = aidi[4]
				raba_id = aidi[5]
				raba_namespace = aidi[6]
				if typ == RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE :
					self.append(value)
				elif typ == RabaFields.RABA_FIELD_TYPE_IS_RABA_LIST :
					raise FutureWarning('RabaList in RabaList not supported')
				elif typ == RabaFields.RABA_FIELD_TYPE_IS_RABA_OBJECT :
					classObj = RabaConnection(raba_namespace).getClass(className)
					self.append(RabaPupa(classObj, raba_id))
				else :
					raise ValueError("Unknown type: %s in rabalist %s" % (typ, self))

	def mutated(self) :
		'returns True if the object has changed since the last save'
		return self._mutated

	def pupatizeElements(self) :
		"""Transform all raba object into pupas"""
		for i in range(len(self)) :
			self[i] = self[i].pupa()

	def empty(self) :
		if self.tableName != None and self.anchorObj != None :
			sql = 'DELETE FROM %s WHERE anchor_raba_id = ?' % self.tableName
			self.connection.execute(sql, (self.anchorObj.raba_id,))

	def _save(self) :
		"""saves the RabaList into it's own table. This a private function that should be called directly
		Before saving the entire list corresponding to the anchorObj is wiped out before being rewritten. The
		alternative would be to keep the sync between the list and the table in real time (remove in both).
		If the current solution proves to be to slow, i'll consider the alternative"""

		if self.connection.registerSave(self) :
			if len(self) == 0 :
				self.connection.updateRabaListLength(self.raba_id, len(self))
				return True
			else :
				if self.relationName == None or self.anchorObj == None :
					raise ValueError('%s has not been attached to any object, impossible to save it' % s)

				if self.raba_id == None :
					self.raba_id, self.tableName = self.connection.registerRabalist(self.anchorObj._rabaClass.__name__, self.anchorObj.raba_id, self.relationName)

				if self._saved :
					self.empty()

				values = []
				for e in self.data :
					if isRabaObject(e) :
						e.save()
						objDct = e.getDctDescription()
						values.append((self.anchorObj.raba_id, None, RabaFields.RABA_FIELD_TYPE_IS_RABA_OBJECT, e._rabaClass.__name__, e.raba_id, e._raba_namespace))
					elif isPythonPrimitive(e) :
						values.append((self.anchorObj.raba_id, e, RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE), None, None, None)
					else :
						values.append((self.anchorObj.raba_id, buffer(cPickle.dumps(e)), RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE, None, None, None))

				self.connection.executemany('INSERT INTO %s (anchor_raba_id, value, type, obj_raba_class_name, obj_raba_id, obj_raba_namespace) VALUES (?, ?, ?, ?, ?, ?)' % self.tableName, values)

				self.connection.updateRabaListLength(self.raba_id, len(self))
				self._saved = True
				self._mutated = False
				return True
		else :
			return False

	def _attachToObject(self, anchorObj, relationName) :
		"Attaches the rabalist to a raba object. Only attached rabalists can  be saved"
		if self.anchorObj == None :
			self.relationName = relationName
			self.anchorObj = anchorObj
			self._setNamespaceConAndConf(anchorObj._rabaClass._raba_namespace)
			self.tableName = self.connection.makeRabaListTableName(self.anchorObj._rabaClass.__name__, self.relationName)
			faultyElmt = self._checkSelf()
			if faultyElmt != None :
				raise ValueError("Element %s violates specified list or relation constraints" % faultyElmt)
		elif self.anchorObj is not anchorObj :
			raise ValueError("Ouch: attempt to steal rabalist, use RabaLict.copy() instead.\nthief: %s\nvictim: %s\nlist: %s" % (anchorObj, self.anchorObj, self))

	def pupa(self) :
		return RabaListPupa(self.namespace, self.anchorObj, self.relationName)

	def _mutateNotifyAnchor(self) :
		self._mutated = True
		self.anchorObj.listsToSave[self.relationName] = self

	def _setNamespaceConAndConf(self, namespace) :
		self._raba_namespace = namespace
		self.connection = RabaConnection(self._raba_namespace)
		self.rabaConfiguration = RabaConfiguration(self._raba_namespace)

	def extend(self, v) :
		faultyElement, namespace = self._checkRabaList(v, self._raba_namespace)
		if faultyElement != None :
			self._dieInvalidRaba(faultyElement)

		self.data.extend(v)
		if self._raba_namespace == None and namespace != None :
			self._setNamespaceConAndConf(namespace)

	#@profile
	def append(self, v) :
		if not self._checkElmt(v) :
			self._dieInvalidRaba(v)

		if self._raba_namespace == None and isRabaObject(v) :
			self._setNamespaceConAndConf(v._raba_namespace)

		self.data.append(v)
		self._mutateNotifyAnchor()

	def insert(self, k, v) :
		if not self._checkElmt(v, self._raba_namespace) :
			self._dieInvalidRaba(v)

		if self._raba_namespace == None and isRabaObject(v) :
			self._setNamespaceConAndConf(v._raba_namespace)

		self.data.insert(k, v)
		self._mutateNotifyAnchor()

	def set(self, lst) :
		self.data = list(lst)
		self._mutateNotifyAnchor()

	def __setitem__(self, k, v) :
		if not self._checkElmt(v, self._raba_namespace) :
			self._dieInvalidRaba(v)

		if self._raba_namespace == None and isRabaObject(v) :
			self._setNamespaceConAndConf(v._raba_namespace)

		self.data[k] = v
		self._mutateNotifyAnchor()

	def __delitem__(self, i) :
		del self.data[i]
		self._mutateNotifyAnchor()

	def __getitem__(self, i) :
		return self.data[i]
		self._mutateNotifyAnchor()

	def __len__(self) :
		return len(self.data)

	def __repr__(self) :
		return '[%s raba_id: %s, len: %d, anchor: %s, table: %s]' % (self._runtimeId, self.raba_id, len(self), self.anchorObj, self.tableName)
