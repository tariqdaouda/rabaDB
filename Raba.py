import sqlite3 as sq
import os, copy, types, cPickle, random, json, abc#, weakref
from collections import MutableSequence

from setup import RabaConnection, RabaConfiguration
import fields as RabaFields

def makeRabaObjectSingletonKey(clsName, namespace, raba_id) :
	return (clsName, namespace, raba_id)

def isRabaObject(v) :
	return hasattr(v, '_rabaClass')
	
def isRabaList(v) :
	return hasattr(v.__class__, '_isRabaList') and v._isRabaList

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
		
		connection = RabaConnection(anchorObj._raba_namespace)
		infos = connection.getRabaListInfos(anchor_class_name = anchorObj._rabaClass.__name__, anchor_raba_id = anchorObj.raba_id, relation_name = relationName)
		
		if infos != None :
			key = infos['raba_id']
		
			if key in RabaListSingleton_Metaclass._instances :
				return RabaListSingleton_Metaclass._instances[key]
			elif key in clsObj._instances :
				return clsObj._instances[key]
			
			for k in kwargs :
				infos[k] = kwargs[k]
			
			obj = super(RabaListPupaSingleton_Metaclass, clsObj).__call__(*args, **infos)
			clsObj._instances[key] = obj
			
			return obj
		return super(RabaListPupaSingleton_Metaclass, clsObj).__call__(*args, **kwargs)
		
class RabaListSingleton_Metaclass(abc.ABCMeta):
	_instances = {}

	def __call__(clsObj, *args, **kwargs):
		
		if 'raba_id' in kwargs :
			key = kwargs['raba_id']
		
			if key in clsObj._instances :
				return clsObj._instances[key]
		
			clsObj._instances[key] = super(RabaList_Metaclass, clsObj).__call__(*args, **kwargs)
		
			#if len(clsObj._instances[key]) > 0 :
			return clsObj._instances[key]
		return super(RabaListSingleton_Metaclass, clsObj).__call__(*args, **kwargs)

class _RabaSingleton_MetaClass(type) :
	
	_instances = {}
	
	def __new__(cls, name, bases, dct) :
		if name != 'Raba' :
			fields = []
			columns = {}
			columnsToLowerCase = {'raba_id' : 'raba_id'}
		
			for k, v in dct.items():
				if RabaFields.isField(v) :
					fields.append(k)
					columns[k.lower()] = -1
					columnsToLowerCase[k.lower()] = k
					
			try :
				con = RabaConnection(dct['_raba_namespace'])
			except KeyError :
				raise ValueError("The class %s has no defined namespace, please add a valid '_raba_namespace' to class attributes" % name)
				
			uniqueStr = ''
			if '_raba_uniques' in dct :
				for c in dct['_raba_uniques'] :
					if len(c) > 1 :
						uniqueStr += 'UNIQUE%s ON CONFLICT REPLACE, ' % str(c)
					else :
						uniqueStr += 'UNIQUE(%s) ON CONFLICT REPLACE, ' % str(c[0])
					
			uniqueStr = uniqueStr[:-2]
				
			if not con.tableExits(name) :
				idStr = 'raba_id INTEGER PRIMARY KEY AUTOINCREMENT'
				if len(fields) > 0 :
					if len(uniqueStr) > -1 :			
						con.createTable(name, '%s, %s' % (idStr, ', '.join(list(fields))))
					else :
						con.createTable(name, '%s, %s, %s' % (idStr, ', '.join(list(fields)), uniqueStr))
				else :
					con.createTable(name, '%s' % idStr)
				
				sqlCons =  'INSERT INTO raba_tables_constraints (table_name, constraints) VALUES (?, ?)'
				con.cursor().execute(sqlCons, (name, uniqueStr))
				#con.commit()
			else :
				cur = con.cursor()
				sql = 'SELECT constraints FROM raba_tables_constraints WHERE table_name = ?'
				cur.execute(sql, (name,))
				res = cur.fetchone()
				
				if res != None and res[0] != uniqueStr :
					raise FutureWarning('Warning: The unique contraints have changed from:\n\t%s\n\nto:\n\t%s.\n-Unique constraints modification is not supported yet-\n' %(res[0], uniqueStr))
					
				cur.execute('PRAGMA table_info("%s")' % name)
				tableColumns = set()
				fieldsToKill = []
				
				for c in cur :
					if c[1] != 'raba_id' and c[1].lower() not in columns :
						#Destroy field that have mysteriously desapeared
						fieldsToKill.append('%s = NULL' % c[1])
						con.dropRabalist(name, c[1])
					else :
						columns[c[1]] = c[0]
					
					tableColumns.add(c[1].lower())
			
				if len(fieldsToKill) > 0 :
					sql = 'UPDATE %s SET %s WHERE 1;' % (name , ', '.join(fieldsToKill))
					cur.execute(sql)
	
				for k in columns :
					if k.lower() not in tableColumns :
						cur.execute('ALTER TABLE %s ADD COLUMN %s' % (name, k))
				#con.commit()
			
			columns['raba_id'] = 0
			dct['raba_id'] = RabaFields.PrimitiveField()
			dct['columns'] = columns 
			dct['columnsToLowerCase'] = columnsToLowerCase
			
			clsObj = type.__new__(cls, name, bases, dct)
			#RabaConfiguration(dct['_raba_namespace']).registerRabaClass(clsObj)
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
			if isRabaObject(v) :
				params[p] = v.getJsonEncoding()
			
		connection = RabaConnection(cls._raba_namespace)
		cur = connection.getRabaObjectInfos(cls.__name__, params)
		dbLine = cur.fetchone()
		
		if dbLine != None :
			if cur.fetchone() != None :
				raise KeyError("More than one object fit the arguments you've prodided to the constructor")
			
			if 'raba_id' in fieldsDct and res == None :
				raise KeyError("There's no %s with a raba_id = %s" %(self._rabaClass.__name__, fieldsDct['raba_id']))
			
			raba_id = dbLine[cls.columns['raba_id']]
			key = makeRabaObjectSingletonKey(cls.__name__, cls._raba_namespace, raba_id)
			if key in cls._instances :
				return cls._instances[key]
			
			obj = type.__call__(cls, initDbLine = dbLine, **fieldsDct)	
			cls._instances[key] = obj					
		elif len(params) > 0 :
			raise KeyError("Couldn't find any object that fit the arguments you've prodided to the constructor")
			#obj = type.__call__(cls, **fieldsDct)
		else :
			obj = type.__call__(cls, **fieldsDct)
		
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
		
	def __getattr__(self, name) :
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
		
		rabaClass.__init__(self, initDbLine = dbLine)
		
		return object.__getattribute__(self, name)
	
	def __repr__(self) :
		return "<Raba pupa: %s, raba_id %s>" % (self._rabaClass.__name__, self.raba_id)

class Raba(object):
	"All raba object inherit from this class"	
	__metaclass__ = _RabaSingleton_MetaClass
	
	def _initDbLine(self, dbLine) :
		for kk, i in self.columns.items() :
			k = self.columnsToLowerCase[kk.lower()]
			elmt = getattr(self._rabaClass, k)
			if RabaFields.typeIsPrimitive(elmt) :
				try :
					self.__setattr__(k, cPickle.loads(str(dbLine[i])))
				except :
					self.__setattr__(k, dbLine[i])
			
			elif RabaFields.typeIsRabaObject(elmt) :
				if dbLine[i] != None :
					val = json.loads(dbLine[i])
					objClass = self.connection.getClass(val["className"])
					self.__setattr__(k, RabaPupa(objClass, val["raba_id"]))
			elif RabaFields.typeIsRabaList(elmt) :
				rlp = RabaListPupa(anchorObj = self, relationName = k)
				self.__setattr__(k, rlp)
			else :
				raise ValueError("Unable to set field %s to %s in Raba object %s" %(k, dbLine[i], self._rabaClass.__name__))
	
	def _initWithDct(self, dct) :
		for k, v in dct.items() :
			if k.lower() in self.columns : 
				self.__setattr__(k, v)
			else :
				raise KeyError("Raba object %s has no field %s" %(self._rabaClass.__name__, k))
	
	def __init__(self, **fieldsSet) :
		
		if self.__class__ == Raba :
			raise TypeError('Raba class should never be instanciated, use inheritance')

		self._rabaClass = self.__class__
		
		self.connection = RabaConnection(self._rabaClass._raba_namespace)
		self.rabaConfiguration =  RabaConfiguration(self._rabaClass._raba_namespace)
		
		if 'initDbLine' in fieldsSet and 'initDbLine' != None :
			self._initDbLine(fieldsSet['initDbLine'])
		else :
			self._initWithDct(fieldsSet)
		
		self._runtimeId = (self.__class__.__name__, random.random()) #this is using only during runtime ex, to avoid circular calls
		self.mutated = True #True if needs to be saved

	def setForceSave(self) :
		"Raba is lazy, be default it doesn't save object who haven't been modified. This Forces the object saving regardless of it's current mutation status"
		self.mutated = True
	
	def autoclean(self) :
		"""TODO: Copies the table into a new one droping all the collumns that have all their values to NULL
		and drop the tables that correspond to these tables"""
		raise FutureWarning("sqlite does not support column droping, work aroun not implemented yet")
		
	def pupa(self) :
		"""returns a pupa version of self"""
		return RabaPupa(self.__class__, self.raba_id)
		
	def save(self) :
		self.connection.initateSave(self)
		
		if self.connection.canISave(self) :
			fields = []
			values = []
			rabalists = []
			cur = self.connection.cursor()
			listsToSave = []
			for k, valType in self.__class__.__dict__.items() :
				if RabaFields.isField(valType) and k != 'raba_id':
					val = getattr(self, k)
					if not RabaFields.typeIsRabaList(valType) and val is valType and self.mutated :
						values.append(valType.default)
						fields.append(k)
					elif RabaFields.typeIsPrimitive(valType) and self.mutated :
						if isPythonPrimitive(val):
							values.append(val)
						else :
							values.append(buffer(cPickle.dumps(val)))
						fields.append(k)
					
					elif RabaFields.typeIsRabaObject(valType) :
						if val != None :
							val.save()
							values.append(val.getJsonEncoding())
							fields.append(k)
				
					elif RabaFields.typeIsRabaList(valType) :
						if isRabaList(val) :
							if val.mutated :
								listsToSave.append(val)
								values.append(len(val))
								fields.append(k)
		
			if len(values) > 0 :
				if self.raba_id == None :
					sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ','.join(fields), ','.join(['?' for i in range(len(fields))]))
					cur.execute(sql, values)
					object.__setattr__(self, 'raba_id', cur.lastrowid)
					key = makeRabaObjectSingletonKey(self._rabaClass.__name__, self._raba_namespace, self.raba_id)
					_RabaSingleton_MetaClass._instances[key] = self
				else :
					values.append(self.raba_id)
					sql = 'UPDATE %s SET %s = ? WHERE raba_id = ?' % (self.__class__.__name__, ' = ?, '.join(fields))
					cur.execute(sql, values)
				#self.connection.commit()
			else :
				return False
			
			for l in listsToSave :
				l._save()
			
			self.mutated = False 
			self.connection.freeSave(self)
			
			return True
		return False
	
	def copy(self) :
		v = copy.copy(self)
		v.raba_id = None
		return v
	
	def getDctDescription(self) :
		"returns a dict describing the object"
		return  {'type' : RabaFields.RABA_FIELD_TYPE_IS_RABA_OBJECT, 'className' : self._rabaClass.__name__, 'raba_id' : self.raba_id}
	
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
			if not RabaFields.typeIsRabaList(getattr(self.__class__, k)) :
				classType = getattr(self.__class__, k)
				if not classType.check(vv) :
					raise ValueError("Unable to set '%s' to value '%s'. Constrain function violation" % (k, vv))
			else :
				if vv.__class__ is not RabaList and vv.__class__ is not RabaListPupa :
					try :
						vv = RabaList(v)
					except :
						raise ValueError("Unable to set '%s' to value '%s'. Value is not a valid RabaList" % (k, vv))
				
				currList = getattr(self, k)
				if vv is not currList :
					currList.erase()
					self.connection.unregisterRabalist(anchor_class_name = self.__class__.__name__, anchor_raba_id = self.raba_id, relation_name = k)
				
				vv._attachToObject(self, k)
			
			object.__setattr__(self, 'mutated', True)
		
		object.__setattr__(self, k, vv)
	
	def __getattribute__(self, k) :
		elmt = object.__getattribute__(self, k)
		if RabaFields.typeIsRabaList(elmt) : #if empty
			elmt = RabaListPupa(anchorObj = self, relationName = k)
		elif RabaFields.isField(elmt) :
			elmt = elmt.default
		
		return elmt
		
	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem__(self, k, v) :
		self. __setattr__(k, v)
	
	def __repr__(self) :
		return "<Raba obj: %s, raba_id: %s>" % (self._rabaClass.__name__, self.raba_id)
	
	def help(self) :
		"returns a string of parameters"
		fil = []
		for k, v in self.__class__.__dict__.items() :
			if RabaFields.isField(v) :
				fil.append(k)
 
		return 'Available fields for %s:\n%s' %(self.__class__.__name__, '\n'.join(fil))
	
class RabaListPupa(MutableSequence) :
	
	_isRabaList = True
	__metaclass__ = RabaListPupaSingleton_Metaclass

	def __init__(self, **kwargs) :
		
		self.anchorObj = kwargs['anchorObj']
		self.relationName = kwargs['relationName']
		self._raba_namespace = self.anchorObj._raba_namespace
		connection = RabaConnection(self._raba_namespace)
		self.mutated = False
		
		try :
			self.raba_id = kwargs['raba_id']
			self.length = kwargs['length']
			self.tableName = kwargs['tableName']
		except KeyError :
			infos = connection.getRabaListInfos(relation_name = self.relationName, anchor_class_name = self.anchorObj.__class__.__name__, anchor_raba_id = self.anchorObj.raba_id)
			
			if infos != None :
				self.raba_id = infos['raba_id']
				self.tableName = infos['table_name']
				self.length = infos['length']
			else :
				self.raba_id, self.tableName = None, None 
				self.length = 0
		
	def _morph(self) :
		MutableSequence.__setattr__(self, '__class__', RabaList)
		
		relName = MutableSequence.__getattribute__(self, 'relationName')
		anchorObj = MutableSequence.__getattribute__(self, 'anchorObj')
		namespace = MutableSequence.__getattribute__(self, '_raba_namespace')
		tableName = MutableSequence.__getattribute__(self, 'tableName')
		raba_id = MutableSequence.__getattribute__(self, 'raba_id')
		
		purge = MutableSequence.__getattribute__(self, '__dict__').keys()
		for k in purge :
			delattr(self, k)
		
		RabaList.__init__(self, raba_id = raba_id, namespace = namespace, anchorObj = anchorObj, tableName = tableName)
	
	def __getitem__(self, i) :
		self._morph()
		return self[i]
	 
	def __delitem__(self, i) :
		self._morph()
		self.__delitem__(i)
	
	def __setitem__(self, k, v) :
		self._morph()
		self.__setitem__(k, v)
			
	def insert(k, i, v) :
		self._morph()
		self.insert(i, v)
	
	def _attachToObject(self, *args, **kwargs) :
		"dummy fct for compatibility reasons, a RabaListPupa is attached by default"
		pass

	def _save(self, *args, **kwargs) :
		"dummy fct for compatibility reasons, a RabaListPupa is by default an modified list"
		pass
		
	def __getattr__(self, name) :
		relName = MutableSequence.__getattribute__(self, 'relationName')
		anchorObj = MutableSequence.__getattribute__(self, 'anchorObj')
		MutableSequence.__getattribute__(self, "_morph")()
		self._attachToObject(anchorObj, relName)
		
		return MutableSequence.__getattribute__(self, name)

	def __repr__(self) :
		return "[RLPupa length: %d, relationName: %s, anchorObj: %s, raba_id: %s]" % (self.length, self.relationName, self.anchorObj, self.raba_id)

	def __len__(self) :
		return self.length
		
class RabaList(MutableSequence) :
	"""A RabaList is a list that can only contain Raba objects of the same class or (Pupas of the same class). They represent one to many relations and are stored in separate
	tables that contain only one single line"""
	
	_isRabaList = True
	__metaclass__ = RabaListSingleton_Metaclass
	
	def _checkElmt(self, v, namespace = None) :
		if self.anchorObj != None and self.relationName != None and not getattr(self.anchorObj._rabaClass, self.relationName).check(v) :
			return False
			
		if not isRabaObject(v) or namespace == None or (namespace != None and v._raba_namespace == namespace) :
			return True
		
		return False
		
	def _checkRabaList(self, v, namespace = None) :
		"""Checks and entire list, returns (faultyElmt, list namespace), if the list passes the check, faultyElmt = None"""
		nm = namespace
		for e in v :
			if nm == None and isRabaObject(e) :
				nm = e._raba_namespace
			if not self._checkElmt(e, nm) :
				return (e, nm)
		
		return (None, nm)
	
	def _dieInvalidRaba(self, v) :
		st = """The element %s can't be added to the list, possible causes:
		-The element is a RabaObject wich namespace is different from list's namespace
		-The element violates the constraint function""" % v
		
		raise TypeError(st)
	
	def __init__(self, *listElements, **listArguments) :
		"""To avoid the check of all elements of listElements during initialisation pass : noInitCheck = True as argument  
		It is also possible to define both the anchor object and the namespace durint initalisation using argument keywords: anchorObj and namespace. But only do it 
		if you really now what you are doing."""
		
		self.raba_id = None
		self.relationName = None
		self.tableName = None
		
		if 'anchorObj' in listArguments and listArguments['anchorObj'] != None :
			self.anchorObj = listArguments['anchorObj']
		else :
			self.anchorObj = None
		
		if 'namespace' in listArguments and listArguments['namespace'] != None :
			self._setNamespaceConAndConf(listArguments['namespace'])
		else :
			self._raba_namespace = None
			self.connection = None
			self.rabaConfiguration = None
			
		if 'noInitCheck' not in listArguments and len(listElements) > 0:
			faultyElement, namespace = self._checkRabaList(listElements[0])
			if faultyElement != None :
				self._dieInvalidRaba(check[1])
		
			if self._raba_namespace != None and namespace != None and namespace != self._raba_namespace :
				raise TypeError("Defined namespace %s != elements namespace %s") %(self._raba_namespace, namespace)
			elif self._raba_namespace == None and namespace != None :
				self._setNamespaceConAndConf(namespace)
			
		if len(listElements) > 0 :
			self.data = list(listElements[0])
		else :
			self.data = []
		
		if 'raba_id' in listArguments and listArguments['raba_id'] != None :
			if self.connection == None :
				raise ValueError('Unable to set list, i have an id but no namespace')
			
			infos = self.connection.getRabaListInfos(raba_id = listArguments['raba_id'])
			self.raba_id = infos['raba_id']
			self.relationName = infos['relation_name']
			self.tableName = infos['table_name']
			
			cur = self.connection.cursor()
			cur.execute('SELECT * FROM %s WHERE anchor_raba_id = ?' % self.tableName, (self.anchorObj.raba_id, ))
			for aidi in cur :
				valueOrId = aidi[2]
				typ = aidi[3]
				if typ == RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE :
					self.append(valueOrId)
				elif typ == RabaFields.RABA_FIELD_TYPE_IS_RABA_LIST :
					raise FutureWarning('RabaList in RabaList not supported')
				else :
					self.append(RabaPupa(self.connection.getClass(typ), valueOrId))
		
		self._runtimeId = (self.__class__.__name__, random.random())#this is using only during runtime ex, to avoid circular calls
		self.mutated = True #True if needs to be saved
		
	def pupatizeElements(self) :
		"""Transform all raba object into pupas"""
		for i in range(len(self)) :
			self[i] = self[i].pupa()

	def erase(self) :
		if self.tableName != None and self.anchorObj != None :
			cur = self.connection.cursor()
			sql = 'DELETE FROM %s WHERE anchor_raba_id = ?' % self.tableName
			cur.execute(sql, (self.anchorObj.raba_id,))
			#self.connection.commit()
	
	def _save(self) :
		"""saves the RabaList into it's own table. This a private function that should be called directly
		Before saving the entire list corresponding to the anchorObj is wiped out before being rewritten. The
		alternative would be to keep the sync between the list and the table in real time (remove in both).
		If the current solution proves to be to slow, i'll consider the alternative"""
		
		if self.connection.canISave(self) and self.mutated and len(self) > 0 :
			if self.raba_id == None :
				self.raba_id, self.tableName = self.connection.registerRabalist(self.anchorObj._rabaClass.__name__, self.anchorObj.raba_id, self.relationName)
			
			if self.relationName == None or self.anchorObj == None :
				raise ValueError('%s has not been attached to any object, impossible to save it' % s)
			
			self.erase()
			
			values = []
			for e in self.data :
				if isRabaObject(e) :
					e.save()
					values.append((self.anchorObj.raba_id, e.raba_id, e._rabaClass.__name__))
				elif isPythonPrimitive(e) :
					values.append((self.anchorObj.raba_id, e, RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE))
				else :
					values.append((self.anchorObj.raba_id, buffer(cPickle.dumps(e)), RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE))
			
			self.connection.cursor().executemany('INSERT INTO %s (anchor_raba_id, value_or_raba_id, type) VALUES (?, ?, ?)' % self.tableName, values)
			#self.connection.commit()
			
			self.connection.updateRabaListLength(self.raba_id, len(self))
		
		self.mutated = False
	
	def _attachToObject(self, anchorObj, relationName) :
		"Attaches the rabalist to a raba object. Only attached rabalists can  be saved"
		if self.anchorObj == None :
			self.relationName = relationName
			self.anchorObj = anchorObj
			self._setNamespaceConAndConf(anchorObj._rabaClass._raba_namespace)
		elif self.anchorObj is not anchorObj :
			raise ValueError("Ouch: attempt to steal rabalist, use RabaLict.copy() instead.\nthief: %s\nvictim: %s\nlist: %s" % (anchorObj, self.anchorObj, self))
		
	def pupa(self) :
		return RabaListPupa(self.namespace, self.anchorObj, self.relationName)
	
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

		self.mutated = True
		
	def append(self, v) :
		if not self._checkElmt(v, self._raba_namespace) :
			self._dieInvalidRaba(v)
		
		if self._raba_namespace == None and isRabaObject(v) :
			self._setNamespaceConAndConf(v._raba_namespace)

		self.data.append(v)
		self.mutated = True
	
	def insert(self, k, v) :
		if not self._checkElmt(v, self._raba_namespace) :
			self._dieInvalidRaba(v)
		
		if self._raba_namespace == None and isRabaObject(v) :
			self._setNamespaceConAndConf(v._raba_namespace)

		self.data.insert(k, v)
		self.mutated = True
	
	def __setitem__(self, k, v) :
		if not self._checkElmt(v, self._raba_namespace) :
			self._dieInvalidRaba(v)
			
		if self._raba_namespace == None and isRabaObject(v) :
			self._setNamespaceConAndConf(v._raba_namespace)
		
		self.data[k] = v
		self.mutated = True
	
	def __delitem__(self, i) :
		del self.data[i]
		self.mutated = True
	
	def __getitem__(self, i) :
		#print 'iop', self, i, type(self.data), type(self.data[i]), self.data[i]
		#try :
			#return RabaList(self.data[i], namespace = self._raba_namespace, noInitCheck = True)
		#except TypeError:
		return self.data[i]
	
	def __len__(self) :
		return len(self.data)
			
	def __repr__(self) :
		return '[RL raba_id: %s, len: %d, anchor: %s, table: %s]' % (self.raba_id, len(self), self.anchorObj, self.tableName)
		#return '[RL id:%s, len: %d %s]' % (self.raba_id, len(self), str(self.data))
