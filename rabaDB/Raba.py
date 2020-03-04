import sqlite3 as sq
import os, copy, pickle, random, json, abc, sys
from collections import MutableSequence

from .rabaSetup import RabaConnection, RabaConfiguration
from . import fields as RabaFields

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
	primTypes = [int, int, float, bytes, str, memoryview, type(None)]
	for t in primTypes :
		if isinstance(v, t) :
			return True
	return False

_RabaList_instances = {}
def _registerRabaListInstance(lst, anchorObj, relationName) :
	global _RabaList_instances
	key = (anchorObj._runtimeId, relationName)
	_RabaList_instances[key] = lst

def _unregisterRabaListInstance(lst) :
	global _RabaList_instances
	key = (lst.anchorObj._runtimeId, lst.relationName)
	try :
		del(_RabaList_instances[key])
	except KeyError :
		pass

def _getRabaListInstance(anchorObj, relationName) :
	global _RabaList_instances
	key = (anchorObj._runtimeId, relationName)
	return _RabaList_instances[key]

_RabaObject_instances = {}
def _registerRabaObjectInstance(obj) :
	global _RabaObject_instances
	key = (obj.__class__, obj._raba_namespace, obj.raba_id)
	_RabaObject_instances[key] = obj

def _unregisterRabaObjectInstance(obj) :
	global _RabaObject_instances
	key = (obj.__class__, obj._raba_namespace, obj.raba_id)
	
	if not isRabaObjectPupa(obj) :
		for l in obj.rabaLists :
			_unregisterRabaListInstance(l)
	
	try :
		del(_RabaObject_instances[key])
	except KeyError :
		pass

def _getRabaObjectInstance(cls, namespace, raba_id) :
	global _RabaObject_instances
	key = (cls, namespace, raba_id)
	return _RabaObject_instances[key]

class _RabaListPupaSingleton_Metaclass(abc.ABCMeta):

	def __call__(clsObj, *args, **kwargs):
		anchorObj = kwargs['anchorObj']
		relationName = kwargs['relationName']
		length = kwargs['length']

		try :
			return _getRabaListInstance(anchorObj, relationName)
		except KeyError:
			pass
		
		connection = RabaConnection(anchorObj._raba_namespace)

		obj = super(_RabaListPupaSingleton_Metaclass, clsObj).__call__(*args, anchorObj = anchorObj, relationName = relationName, length = length)

		_registerRabaListInstance(obj, anchorObj, relationName)
	
		return obj

class _RabaListSingleton_Metaclass(abc.ABCMeta):
	def __call__(clsObj, *args, **kwargs):

		if 'anchorObj' in kwargs and 'relationName' in kwargs :
			anchorObj = kwargs['anchorObj']
			relationName = kwargs['relationName']

			try :
				return _getRabaListInstance(anchorObj, relationName)
			except KeyError:
				pass

			obj = super(RabaList_Metaclass, clsObj).__call__(*args, **kwargs)

			_registerRabaListInstance(obj, anchorObj, relationName)
			
		return super(_RabaListSingleton_Metaclass, clsObj).__call__(*args, **kwargs)

class _RabaPupaSingleton_Metaclass(type):
	def __call__(clsObj, *args, **kwargs):

		if 'classObj' in kwargs :
			cls = kwargs['classObj']
		else :
			cls = args[0]

		if 'raba_id' in kwargs :
			raba_id = kwargs['raba_id']
		else :
			raba_id = args[1]

		try :
			return _getRabaObjectInstance(cls, cls._raba_namespace, raba_id)
		except KeyError :
			pass
		
		obj = super(_RabaPupaSingleton_Metaclass, clsObj).__call__(*args, **kwargs) 
		_registerRabaObjectInstance(obj)
		 
		return obj
	
class _RabaSingleton_MetaClass(type) :

	_instances = {}

	def __new__(cls, name, bases, dct) :
		if '_raba_abstract' not in dct or not dct['_raba_abstract'] :
			def getFields_rec(name, sqlFields, columns, columnsToLowerCase, dct, bases) :
				i = 0
				if name != "Raba" :
					for base in bases :
						i += getFields_rec(base.__name__, sqlFields, columns, columnsToLowerCase, base.__dict__, base.__bases__)

				for k, v in dct.items() :
					if RabaFields.isField(v) :
						sk = str(k)
						if k.lower() != 'raba_id' and k.lower() != 'json' :
							sqlFields.append(sk)

						columns[sk] = i
						columnsToLowerCase[sk.lower()] = sk
						i += 1
				return i

			sqlFields = []
			columns = {}
			columnsToLowerCase = {}
			getFields_rec(name, sqlFields, columns, columnsToLowerCase, dct, bases)
			columns["raba_id"] = 0
			columns['json'] = 1
			
			try :
				con = RabaConnection(dct['_raba_namespace'])
			except KeyError :
				raise ValueError("The class %s has no defined namespace, please add a valid '_raba_namespace' to class attributes" % name)

			uniqueStr = ''
			if '_raba_uniques' in dct :
				for c in dct['_raba_uniques'] :
					if type(c) is str :
						uniqueStr += 'UNIQUE (%s) ON CONFLICT REPLACE, ' % c
					elif len(c) == 1 :
						uniqueStr += 'UNIQUE (%s) ON CONFLICT REPLACE, ' % c[0]
					else :
						uniqueStr += 'UNIQUE %s ON CONFLICT REPLACE, ' % str(c)
			uniqueStr = uniqueStr[:-2]

			if not con.tableExits(name) :
				idJsonStr = 'raba_id INTEGER PRIMARY KEY, json '
				if len(sqlFields) > 0 :
					if len(uniqueStr) > 0 :
						con.createTable(name, '%s, %s, %s' % (idJsonStr, ', '.join(list(sqlFields)), uniqueStr))
					else :
						con.createTable(name, '%s, %s' % (idJsonStr, ', '.join(list(sqlFields))))
				else :
					con.createTable(name, '%s' % idJsonStr)

				sqlCons = 'INSERT INTO raba_tables_constraints (table_name, constraints) VALUES (?, ?)'
				con.execute(sqlCons, (name, uniqueStr))
			else :
				sql = 'SELECT constraints FROM raba_tables_constraints WHERE table_name = ?'

				cur = con.execute(sql, (name,))
				res = cur.fetchone()

				if res != None and res[0]!= '' and res[0] != uniqueStr :
					sys.stderr.write('Warning: The unique contraints have changed from:\n\t%s\n\nto:\n\t%s.\n-Unique constraints modification is not supported yet-\n' %(res[0], uniqueStr))
					#raise FutureWarning('Warning: The unique contraints have changed from:\n\t%s\n\nto:\n\t%s.\n-Unique constraints modification is not supported yet-\n' %(res[0], uniqueStr))

				cur = con.execute('PRAGMA table_info("%s")' % name)
				tableColumns = set()
				tableColumnsToKeep = []
				tableColumnsToDrop = []

				mustClean = False
				columns = {}
				columns['raba_id'] = 0
				columns['json'] = 1
				i = len(columns)
				for c in cur :
					if c[1] != 'raba_id' and c[1] != 'json' :
						if c[1].lower() not in columnsToLowerCase :
							mustClean = True
							con.dropRabalist(name, c[1])
							tableColumnsToDrop.append(c[1])
						else :
							tableColumnsToKeep.append(c[1])
							columns[columnsToLowerCase[c[1].lower()]] = i
							i += 1
					tableColumns.add(c[1].lower())

				if mustClean :
					con.dropColumnsFromRabaObjTable(name, tableColumnsToKeep)

				mustAlter = False
				for k in columnsToLowerCase :
					if k not in tableColumns :
						con.execute('ALTER TABLE %s ADD COLUMN %s' % (name, columnsToLowerCase[k]))
						mustAlter = True

				if mustClean or mustAlter :
					con.forceCommit()

			dct['columns'] = columns
			dct['columnsToLowerCase'] = columnsToLowerCase

			clsObj = type.__new__(cls, name, bases, dct)
			con.registerRabaClass(clsObj)
			return clsObj

		return type.__new__(cls, name, bases, dct)

	def __call__(cls, *args, **fieldsDct) :
		if cls == Raba :
			return super(_RabaSingleton_MetaClass, cls).__call__(**fieldsDct)

		if 'raba_id' in fieldsDct :
			try :
				return _getRabaObjectInstance(cls, cls._raba_namespace, raba_id)
			except KeyError :
				pass
		else :
			key = None

		params = copy.copy(fieldsDct)
		nonRabaParams = {}
		for p, v in params.items() :
			if p in cls.columns :
				if isRabaObject(v) :
					params[p] = v.getJsonEncoding()
			else :
				nonRabaParams[p] = v
				del(params[p])

		connection = RabaConnection(cls._raba_namespace)
		ret = connection.getRabaObjectInfos(cls.__name__, params)
		
		if ret != None :
			dbLine = ret.fetchone()
		else :
			dbLine = None

		if dbLine != None :
			if ret.fetchone() != None :
				raise ValueError("More than one object fit the arguments you've provided to the constructor")

			raba_id = dbLine[0]
			try :
				return _getRabaObjectInstance(cls, cls._raba_namespace, raba_id)
			except KeyError :
				pass

			obj = Raba.__new__(cls, *args, **nonRabaParams)
			obj._raba__init__(initDbLine = dbLine)
			obj.__init__(*args, **nonRabaParams)

			if not hasattr(cls, '_raba_not_a_singleton') or not getattr(cls, '_raba_not_a_singleton') :
				_registerRabaObjectInstance(obj)
		
		elif len(params) > 0 : # params provided but no result
			raise KeyError("Couldn't find any object that fit the arguments you've provided to the constructor")
		else :
			obj = type.__call__(cls, *args, **fieldsDct)
			obj._raba__init__(**fieldsDct)

		return obj

def freeRegistery() :
	"""Empties all registeries. This is useful if you want to allow the garbage collector to free the memory
	taken by the objects you've already loaded. Be careful might cause some discrepenties in your scripts"""
	freeListRegistery()
	freeObjectRegistery()
	
def freeListRegistery() :
	"""same as freeRegistery() bu only for lists"""
	global _RabaList_instances
	_RabaList_instances = {}
	
def freeObjectRegistery() :
	"""same as freeRegistery() bu only for objects"""
	global _RabaObject_instances
	_RabaObject_instances = {}

def removeFromRegistery(obj) :
	"""Removes an object/rabalist from registery. This is useful if you want to allow the garbage collector to free the memory
	taken by the objects you've already loaded. Be careful might cause some discrepenties in your scripts. For objects,
	cascades to free the registeries of related rabalists also"""
	
	if isRabaObject(obj) :
		_unregisterRabaObjectInstance(obj)
	elif isRabaList(obj) :
		_unregisterRabaListInstance(obj)	

class RabaPupa(object, metaclass=_RabaPupaSingleton_Metaclass) :
	"""One of the founding principles of RabaDB is to separate the storage from the code. Fields are stored in the DB while the processing only depends
	on your python code. This approach ensures a higher degree of stability by preventing old objects from lurking inside the DB before popping out of nowhere several decades afterwards.
	According to this apparoach, raba objects are not serialised but transformed into pupas before being stored. A pupa is a very light object that contains only a reference
	to the raba object class, and it's unique raba_id. Upon asking for one of the attributes of a pupa, it magically transforms into a full fledged raba object. This process is completly transparent to the user. Pupas also have the advantage of being light weight and also ensure that the only raba objects loaded are those explicitely accessed, thus potentialy saving a lot of memory.
	For a pupa self._rabaClass refers to the class of the object "inside" the pupa.
	"""

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
		for k in list(purge) :
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
		return json.dumps(self.getDctDescription(), sort_keys=True)  # sort_keys added during migration to python3

	def __getattr__(self, name) :
		develop = object.__getattribute__(self, "develop")
		develop()

		return self.__getattribute__(name)

	def __str__(self) :
		self.develop()
		return str(self)

	def __repr__(self) :
		return "<RabaObj pupa: %s, raba_id %s>" % (self._rabaClass.__name__, self.raba_id)

class Raba(object, metaclass=_RabaSingleton_MetaClass):
	"All raba object inherit from this class"
	raba_id = RabaFields.Primitive()
	json = RabaFields.Primitive()
	_raba_abstract = True

	def __init__(self, *a, **b) :
		pass

	def unreference(self) :
		"explicit deletes the object from the singleton reference dictionary. This is mandatory to be able to delete the object using del(). Also, any attempt to reload an object with the same parameters will result un a new instance being created"
		try :
			del(self.__class__._instances[makeRabaObjectSingletonKey(self.__class__.__name__, self._raba_namespace, self.raba_id)])
		except KeyError :
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
					self.__setattr__(k, pickle.loads(str(dbLine[i])))
				except :
					self.__setattr__(k, dbLine[i])

			elif RabaFields.isRabaObjectField(elmt) :
				if dbLine[i] != None :
					val = json.loads(dbLine[i])
					objClass = RabaConnection(val["raba_namespace"]).getClass(val["className"])
					self.__setattr__(k, RabaPupa(objClass, val["raba_id"]))
			elif RabaFields.isRabaListField(elmt) :
				if dbLine[i] == None :
					lists.append((k, 0))
				else :
					lists.append((k, int(dbLine[i])))
			else :
				raise ValueError("Unable to set field %s to %s in Raba object %s" %(k, dbLine[i], self._rabaClass.__name__))

		#~ self.rabaLists = []
		for k, leng in lists :
			rlp = RabaListPupa(anchorObj = self, relationName = k, length = leng)
			self.__setattr__(k, rlp)
			self.rabaLists.append(rlp)

	def _raba__init__(self, **fieldsSet) :

		self.sqlSave = {}
		self.sqlSaveQMarks = {}
		self.listsToSave = {}
		self.rabaLists = []
	
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
			self.raba_id = self.connection.getNextRabaId(self)
		
	def pupa(self) :
		"""returns a pupa version of self"""
		return RabaPupa(self.__class__, self.raba_id)

	def develop(self) :
		"Dummy fct, so when you call develop on a full developed object you don't get nasty exceptions"
		pass
	
	@classmethod
	def _parseIndex(cls, fields) :
		con = RabaConnection(cls._raba_namespace)
		ff = []
		rlf = []
		tmpf = []
		if type(fields) is str :
			tmpf.append(fields)
		else :
			tmpf = fields
			
		for field in tmpf :	
			if RabaFields.isRabaListField(getattr(cls, field)) :
				lname = con.makeRabaListTableName(cls.__name__, field)
				rlf.append(lname, )
			else :
				ff.append(field)
		
		return rlf, ff
		
	@classmethod
	def ensureIndex(cls, fields, where = '', whereValues = []) :
		"""Add an index for field, indexes take place and slow down saves and deletes but they speed up a lot everything else. If you are going to do a lot of saves/deletes drop the indexes first re-add them afterwards
		Fields can be a list of fields for Multi-Column Indices or simply the name of a single field. But as RabaList are basicaly in separate tables you cannot create a multicolumn indice on them. A single index will
		be create for the RabaList alone"""
		con = RabaConnection(cls._raba_namespace)
		rlf, ff = cls._parseIndex(fields)
		ww = []
		for i in range(len(whereValues)) :
			if isRabaObject(whereValues[i]) :
				ww.append(whereValues[i].getJsonEncoding())

		for name in rlf :
			con.createIndex(name, 'anchor_raba_id')
		
		if len(ff) > 0 :
			con.createIndex(cls.__name__, ff, where = where, whereValues = ww)
		con.commit()

	@classmethod
	def dropIndex(cls, fields) :
		"removes an index created with ensureIndex "
		con = RabaConnection(cls._raba_namespace)
		rlf, ff = cls._parseIndex(fields)
		
		for name in rlf :
			con.dropIndex(name, 'anchor_raba_id')
		
		con.dropIndex(cls.__name__, ff)
		con.commit()
	
	@classmethod
	def getIndexes(cls) :
		"returns a list of the indexes of a class"
		con = RabaConnection(cls._raba_namespace)
		idxs = []
		for idx in con.getIndexes(rabaOnly = True) :
			if idx[2] == cls.__name__ :
				idxs.append(idx)
			else :
				for k in cls.columns :
					if RabaFields.isRabaListField(getattr(cls, k)) and idx[2] == con.makeRabaListTableName(cls.__name__, k) :
						idxs.append(idx)
		return idxs
	
	@classmethod
	def flushIndexes(cls) :
		"drops all indexes for a class"
		con = RabaConnection(cls._raba_namespace)
		for idx in cls.getIndexes() :
			con.dropIndexByName(idx[1])

	def mutated(self) :
		'returns True if the object has changed since the last save'
		return len(self.sqlSave) > 0 or len(self.listsToSave) > 0

	def save(self) :
		if self.mutated() :
			if not self.raba_id :
				raise ValueError("Field raba_id of self has the not int value %s therefore i cannot save the object, sorry" % (self, self.raba_id))
			
			for k, v in self.listsToSave.items() :
				v._save()
				self.sqlSave[k] = len(v)
				if not self._saved : #this dict is only for optimisation purpose for generating the insert sql
					self.sqlSaveQMarks[k] = '?'

			self.sqlSave['json'] = self.getJsonEncoding()
			if not self._saved : #this dict is only for optimisation purpose for generating the insert sql
				self.sqlSaveQMarks['json'] = '?'

			if not self._saved :
				values = list(self.sqlSave.values())
				sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ', '.join(list(self.sqlSave.keys())), ', '.join(list(self.sqlSaveQMarks.values())))
			else :
				values = list(self.sqlSave.values())
				sql = 'UPDATE %s SET %s = ? WHERE raba_id = ?' % (self.__class__.__name__, ' = ?, '.join(list(self.sqlSave.keys())))
				values.append(self.raba_id)

			self.connection.execute(sql, values)
			self.connection.commit()
			self._saved = True
			self.sqlSave = {}
			self.sqlSaveQMarks = {}
			self.listsToSave = {}

	def delete(self) :
		if self._saved :
			for c in self.columnsToLowerCase.values() :
				if isRabaList(getattr(self, c)) :
					getattr(self, c).empty()
			self.connection.delete(table = self.__class__.__name__, where = 'raba_id = ?', values = (self.raba_id, ))
			self.connection.commit()

	def copy(self) :
		v = copy.copy(self)
		v.raba_id = None
		return v

	def getDctDescription(self) :
		"returns a dict describing the object"
		return  {'type' : RabaFields.RABA_FIELD_TYPE_IS_RABA_OBJECT, 'className' : self._rabaClass.__name__, 'raba_id' : self.raba_id, 'raba_namespace' : self._raba_namespace}

	def getJsonEncoding(self) :
		"returns a json encoding of self.getDctDescription()"
		return json.dumps(self.getDctDescription(), sort_keys=True)  # sort_keys added during migration to python3

	def set(self, **args) :
		"set multiple values quickly, ex : name = woopy"
		for k, v in args.items() :
			setattr(self, k, v)

	def __setattr__(self, k, v) :
		"This also keeps track of wich fields have been updated."
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
					vSQL = memoryview(pickle.dumps(vv))

				self.sqlSave[k] = vSQL

				if not self._saved : #this dict is only for optimisation purpose for generating the insert sql
					self.sqlSaveQMarks[k] = '?'
			else :
				if not isRabaList(vv) and not isRabaListPupa(vv) :
					try :
						vv = RabaList(v)
					except :
						raise ValueError("Unable to set '%s' to value '%s'. Value is not a valid RabaList" % (k, vv))

				currList = object.__getattribute__(self, k)
				if not RabaFields.isRabaListField(currList) and vv is not currList and len(currList) > 0 :
					currList.erase()
					self.connection.unregisterRabalist(anchor_class_name = self.__class__.__name__, anchor_raba_id = self.raba_id, relation_name = k)

				vv._attachToObject(self, k)
				self.listsToSave[k] = vv #self.sqlSave[k] and self.sqlSaveQMarks[k] are updated in self.save() for lists

		object.__setattr__(self, k, vv)

	def __getattribute__(self, k) :
		try :
			elmt = object.__getattribute__(self, k)
			if RabaFields.isRabaListField(elmt) : #if empty
				elmt = RabaListPupa(anchorObj = self, relationName = k, length = -1)
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

	@classmethod
	def getFields(cls) :
		"""returns a set of the available fields. In order to be able ti securely loop of the fields, "raba_id" and "json" are not included in the set"""
		s = set(cls.columns.keys())
		s.remove('json')
		s.remove('raba_id')
		return s
	
	@classmethod
	def help(cls) :
		"returns a string of lisinting available fields"
		return 'Available fields for %s: %s' %(cls.__name__, ', '.join(cls.getFields()))

class RabaListPupa(MutableSequence, metaclass=_RabaListPupaSingleton_Metaclass) :

	_isRabaList = True

	def __init__(self, **kwargs) :
		self._runtimeId = (self.__class__.__name__, random.random()) #this is using only during runtime ex, to avoid circular calls
		self.anchorObj = kwargs['anchorObj']
		self.relationName = kwargs['relationName']
		self.length = kwargs['length']
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
		initFromPupa['length'] = MutableSequence.__getattribute__(self, 'length')
		#initFromPupa['raba_id'] = MutableSequence.__getattribute__(self, 'raba_id')

		purge = MutableSequence.__getattribute__(self, '__dict__').keys()
		for k in list(purge) :
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
		if self.length < 0 :
			self.develop()
		return self.length

class RabaList(MutableSequence, metaclass=_RabaListSingleton_Metaclass) :
	"""A RabaList is a list that can only contain Raba objects of the same class or (Pupas of the same class). They represent one to many relations and are stored in separate
	tables that contain only one single line"""

	_isRabaList = True

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
			#self.raba_id = pupaInit['raba_id']
			self.tableName = pupaInit['tableName']
			length = pupaInit['length']

			self._setNamespaceConAndConf(self.anchorObj._raba_namespace)
			self.connection.createRabaListTable(self.tableName)
			#if self.raba_id == None :
			#	self.raba_id, self.tableName = self.connection.registerRabalist(self.anchorObj._rabaClass.__name__, self.anchorObj.raba_id, self.relationName)

			if length > 0 :
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

				#if self.raba_id == None :
				#	self.raba_id, self.tableName = self.connection.registerRabalist(self.anchorObj._rabaClass.__name__, self.anchorObj.raba_id, self.relationName)

				if self._saved :
					self.empty()

				values = []
				for e in self.data :
					if isRabaObject(e) :
						e.save()
						objDct = e.getDctDescription()
						values.append((self.anchorObj.raba_id, None, RabaFields.RABA_FIELD_TYPE_IS_RABA_OBJECT, e._rabaClass.__name__, e.raba_id, e._raba_namespace))
					elif isPythonPrimitive(e) :
						values.append((self.anchorObj.raba_id, e, RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE, None, None, None))
					else :
						values.append((self.anchorObj.raba_id, memoryview(pickle.dumps(e)), RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE, None, None, None))

				self.connection.executeMany('INSERT INTO %s (anchor_raba_id, value, type, obj_raba_class_name, obj_raba_id, obj_raba_namespace) VALUES (?, ?, ?, ?, ?, ?)' % self.tableName, values)

				#self.connection.updateRabaListLength(self.raba_id, len(self))
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
		return '[ %s, len: %d, anchor: %s, table: %s]' % (self._runtimeId, len(self), self.anchorObj, self.tableName)
