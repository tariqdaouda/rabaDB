import sqlite3 as sq
import os, types, cPickle, random
from setup import RabaConnection


def getClassTYPES() :
	"Returns the sub classes of Raba that have been imported. Warning if classes have not been imported, there's no way for python to know about them"
	types = set()
	for c in Raba.__subclasses__() :
		types.add(c.__name__)
	return types

def isRabaList(v) :
	return v.__class__ == RabaList

def isRabaField(v) :
	return v.__class__ == RabaField
	
def isDbField(v) :
	return v.__class__ == RabaField or v.__class__ == RabaList
	
def isRabaClass(v) :
	return hasattr(v, '__class__') and hasattr(v.__class__, '_isRabaClass') and v.__class__._isRabaClass
	
def isPrimitiveType(v) :
	primTypes = [types.IntType, types.LongType, types.FloatType, types.StringType, types.UnicodeType, types.BufferType, types.NoneType]
	for t in primTypes :
		if isinstance(v, t) : 
			return True
	return False

class _Raba_MetaClass(type) :
	def __new__(cls, name, bases, dct) :
		if name != 'Raba' :
			fields = []
			_fieldsLowCase = {}
			autoIncr = True
			
			#if 'raba_id' not in dct :
			#	dct['raba_id'] = Autoincrement
				
			for k, v in dct.items():
				#if k[0] != '_' and k != 'raba_id' :
				if isDbField(v) :
					fields.append(k)
					_fieldsLowCase[k.lower()] = k 
			
			uniqueStr = ''
			for c in dct['_raba_uniques'] :
				if len(c) > 1 :
					uniqueStr += 'UNIQUE%s ON CONFLICT REPLACE, ' % str(c)
				else :
					uniqueStr += 'UNIQUE(%s) ON CONFLICT REPLACE, ' % str(c[0])
					
			uniqueStr = uniqueStr[:-2]
			dct['_fieldsLowCase'] = _fieldsLowCase
			
			idStr = 'raba_id INTEGER PRIMARY KEY AUTOINCREMENT'
			
			con = RabaConnection(dct['_raba_namespace'])
			
			#if name != 'Raba' and not con.tableExits(name) :
			if not con.tableExits(name) :
				if len(fields) > 0 :
					#sql = 'CREATE TABLE %s (%s, %s)' % (name, idStr, ', '.join(list(fields)))
					con.createTable(name, '%s, %s, %s' % (idStr, ', '.join(list(fields)), uniqueStr))
				else :
					#sql = 'CREATE TABLE %s (%s)' % (name, idStr)
					con.createTable(name, '%s' % idStr)
				
			def _getAttr(self, k) :
				try :
					#print getattr(self, self._fieldsLowCase[k.lower()])
					return getattr(self, self._fieldsLowCase[k.lower()])
				except :
					raise AttributeError("Raba type '%s' has no attribute '%s'" % (self.__name__, k))
			
			cls.__getattr__ = _getAttr
			
		return type.__new__(cls, name, bases, dct)

class RabaField(object) :
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		self.default = default
		self.constrainFct = constrainFct
		self.constrainFctArgs = constrainFctArgs
		self.constrainFctWArgs = constrainFctWArgs
		
	def check(self, val) :
		if self.constrainFct == None :
			return True
		
		return self.constrainFct(val, *self.constrainFctArgs, **self.constrainFctWArgs)
	
class RabaPupa(object) :
	"""One of the founding principles of RabaDB is to separate the storage from the code. Fields are stored in the DB while the processing only depends
	on your python code. This approach ensures a higher degree of stability by preventing old objects from lurking inside the DB before popping out of nowhere several decades afterwards. 
	According to this apparoach, raba objects are not serialised but transformed into pupas before being stored. A pupa is a very light object that contains only a reference
	to the raba object class, and it's unique raba_id. Upon asking for one of the attributes of a pupa, it magically transforms into a full fledged raba object. This process is completly transparent to the user. Pupas also have the advantage of being light weight and also ensure that the only raba objects loaded are those explicitely accessed, thus potentialy saving a lot of memory.
	For a pupa self._rabaClass refers to the class of the object "inside" the pupa.
	"""
	_isRabaClass = True

	def __init__(self, classObj, uniqueId) :
		self._rabaClass = classObj
		self.raba_id = uniqueId
		self.__doc__ = classObj.__doc__
		self.bypassMutationAttr = set(['_isRabaClass', '_rabaClass', 'raba_id', '__class__', '__doc__'])
			
	def __getattribute__(self, name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
	
		if name in getAttr('bypassMutationAttr'):
			return object.__getattribute__(self, name)
		
		setAttr('__class__', getAttr('_rabaClass'))
		uniqueId = getAttr('raba_id')
		
		purge = getAttr('__dict__').keys()
		for k in purge :
			delattr(self, k)
			
		Raba.__init__(self, uniqueId)
		
		return object.__getattribute__(self, name)
	
	def __repr__(self) :
		return "<Raba pupa: %s, raba_id %s>" % (self._rabaClass.__name__, self.raba_id)

class Raba(object):
	"All raba object must inherit from this class. If the class has no attribute raba_id, an autoincrement field raba_id will be created"	
	__metaclass__ = _Raba_MetaClass
	_isRabaClass = True
	
	def __init__(self, **fieldsSet) :#, uniqueId = None) :
		
		if self.__class__ == Raba :
			raise TypeError('Raba class should never be instanciated, use inheritance')

		self._rabaClass = self.__class__
		
		self.connection = RabaConnection(self._rabaClass._raba_namespace)
		self.columns = {}
		self.columnsLowCase = set()
		cur = self.connection.cursor()
		cur.execute('PRAGMA table_info(%s)' % self.__class__.__name__ )
		
		#Destroy field that have mysteriously desapeared
		for c in cur :
			#print c
			if c[1] != 'raba_id'  and c[1].lower() not in self.__class__._fieldsLowCase :
				tableName = self.connection.getRabaListTableName(self._rabaClass, c[1])
				if tableName != None :
					self.connection.dropTable(tableName)
					self.connection.unregisterRabaList(self._rabaClass, c[1])
					
				cur.execute('UPDATE %s SET %s=NULL WHERE 1;' % (self.__class__.__name__ , c[1]))
			else :
				self.columns[c[0]] = c[1]
				self.columnsLowCase.add(c[1].lower())
		self.connection.commit()
		
		#Initialisation
		self.raba_id = None
		
		definedFields = []
		definedValues = []
		for k, v in fieldsSet.items() :
			if k in self.__class__._fieldsLowCase :
				object.__setattr__(self, k, v)
				definedFields.append(k)
				definedValues.append(v)
				
		strWhere = ''
		for k in definedFields :
			strWhere = '%s = ? AND' % k
		
		strWhere = strWhere[:-4]
		sql = 'SELECT * FROM %s WHERE %s' % (self.__class__.__name__, strWhere)
		cur = self.connection.cursor()
		cur.execute(sql, definedValues)
		res = cur.fetchone()
		if cur.fetchone() != None :
			raise KeyError("More than one object fit the arguments you've prodided to constructor")
		
		if res != None :
			for i in self.columns :
				if self.columns[i] != 'raba_id' :
					elmt = getattr(self.__class__, self.columns[i])
					if isRabaField(elmt) :
						object.__setattr__(self, self.columns[i], res[i])
				else :
					object.__setattr__(self, self.columns[i], res[i])
	
	def autoclean(self) :
		"""TODO: Copies the table into a new one removing all the collumns that have all their values to NULL
		and drop the tables that correspond to these tables"""
		pass
	
	def pupa(self) :
		"""returns a pupa version of self"""
		return RabaPupa(self.__class__, self.raba_id)
		
	def save(self) :
		fields = []
		values = []
		rabalists = []
		cur = self.connection.cursor()
		for k, v in self.__class__.__dict__.items() :
			if isDbField(v): #k[0] != '_'  and k != 'raba_id' :
				
				if k in self.__dict__ :
					val = self.__dict__[k]
				else :
					val = v
				#print  k, self.__class__._fieldsLowCase[k.lower()], self.__class__._fieldsLowCase[k.lower()] in self.__dict__, isRabaList(val)
				if not isinstance(val, types.FunctionType) :
					if k.lower() not in (self.columnsLowCase) :
						sql = 'ALTER TABLE %s ADD %s;' % (self.__class__.__name__, k.lower())
						self.connection.cursor().execute(sql)
					
					fields.append(k)
					if isPrimitiveType(val) :
						values.append(val)
					elif isRabaList(val) :
						rabalists.append((k, val))
						values.append(len(val))
					elif isRabaField(val) :
						values.append(val.default)
					else :
						#serialize
						values.append(buffer(cPickle.dumps(val)))
		
		#print rabalists
		if len(values) > 0 :
			if self._newEntry :
				questionMarks = []
				if self.__class__.raba_id is Autoincrement and not self._idIsSet :
					for i in range(len(values)) :
						questionMarks.append('?')
					sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ','.join(fields), ','.join(questionMarks))
					cur.execute(sql, values)
					self.raba_id = cur.lastrowid
					self._idIsSet = True
				else :
					
					if self.raba_id == None or self.raba_id == '' :
						raise ValueError("I can't save a RabaFieldect (%s) whose raba_id value is None or ''" % self)
					
					fields.append('raba_id')
					values.append(self.raba_id)
					for i in range(len(values)) :
						questionMarks.append('?')
					sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ','.join(fields), ','.join(questionMarks))
					cur.execute(sql, values)
			else :
				sql = 'UPDATE %s SET %s = ? WHERE raba_id = ?' % (self.__class__.__name__, ' = ?, '.join(fields))
				values.append(self.raba_id)
				cur.execute(sql, values)
		else :
			raise ValueError('class %s has no fields to save' % self.__class__.__name__)
		
		for relation, l in rabalists :
			l._save(relation, self)
			
		self.connection.commit()

	def __setattr__(self, k, v) :
		if k == 'raba_id' and k in self.__dict__ and self.__dict__[k] != None :
			raise KeyError("You cannot change the raba_id once it has been set ( %s => %s, obj: %s)." % (self.raba_id, v, self))
		#elif hasattr(self.__class__, k) and isRabaField(getattr(self.__class__, k)) and not isRabaList(v) : #and not isRabaClass(v) 
		#	raise TypeError("I'm sorry but you can't replace a raba type by someting else (%s: from %s to %s)" %(k, getattr(self.__class__, k), v))
		elif hasattr(self.__class__, k) and isRabaField(getattr(self.__class__, k)) :
			rf = getattr(self.__class__, k)
			if not rf.check(v) :
				raise ValueError("Unable to set '%s' to value '%s'. Constrain function violation" % (k, v))
		else :
			object.__setattr__(self, k, v)
	
	def __getattribute__(self, k) :
		elmt = object.__getattribute__(self, k)
		if isRabaField(elmt) :
			elmt = elmt.default
		#transform into a RabaListPupa
		#if isRabaField(elmt) :
		#	elmt = RabaListPupa(indexedClass = elmt._rabaClass, relationName = k, anchorObj = self)
		#	setattr(self, k, elmt)
		
		return elmt
		
	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __hash__(self) :
		return self.__class__.__name__+str(self.uniqueId)
	
	def __repr__(self) :
		return "<Raba obj: %s, raba_id %s>" % (self._rabaClass.__name__, self.raba_id)
	
class RabaListPupa(list) :
	_isRabaList = True
	
	def __init__(self, namespace, indexedClass, relationName, anchorObj) :
		self._raba_namespace = namespace
		self.relationName = relationName
		self.anchorObj = anchorObj
		self.indexedClass = indexedClass
		self.bypassMutationAttr = set(['relationName', 'anchorObj', 'indexedClass', '__class__', '_morph'])
		
	def _morph(self) :
		def getAttr(name) :
			return list.__getattribute__(self, name)
			
		def setAttr(name, value) :
			list.__setattr__(self, name, value)
		
		setAttr('__class__', RabaList)
		
		indC = getAttr('indexedClass')
		relName = getAttr('relationName')
		anchObj = getAttr('anchorObj')
		namespace =  getAttr('_raba_namespace')
		
		purge = getAttr('__dict__').keys()
		for k in purge :
			delattr(self, k)
		
		RabaList.__init__(self, namespace, indexedClass = indC, relationName = relName, anchorObj = anchObj)
		
	def __getitem__(self, k) :
		self._morph()
		return self[k]
		
	def __getattribute__(self, name) :
		if name in list.__getattribute__(self, "bypassMutationAttr") :
			return list.__getattribute__(self, name)
		
		list.__getattribute__(self, "_morph")()
		
		return list.__getattribute__(self, name)

	def __repr__(self) :
		return "<RLPupa relationName: %s, indexedClass: %s, anchorObj:%s>" % (self.relationName, self.indexedClass.__name__, self.anchorObj)

class RabaList(list) :
	"""A RabaList is a list that can only contain Raba objects of the same class or (Pupas of the same class). They represent one to many relations and are stored in separate
	tables that contain only one single line"""
	
	_isRabaList = True
	
	def _checkElmt(self, v) :
		if not isRabaClass(v) :
			return False
			
		if len(self) > 0 and v._rabaClass != self[0]._rabaClass or  v._raba_namespace != self._raba_namespace :
			return False
		
		return True
		
	def _checkRabaList(self, v) :
		vv = list(v)
		for e in vv :
			if not self._checkElmt(e) :
				return (False, e)
		return (True, None)
	
	def _dieInvalidRaba(self, v) :
		raise TypeError('Only Raba objects of the same class can be stored in RabaLists and into the same namespace. Elmt: %s is not a valid RabaFieldect' % v)
			
	def __init__(self, namespace, *argv, **argk) :
		list.__init__(self, *argv)
		check = self._checkRabaList(self)
		if not check[0]:
			self._dieInvalidRaba(check[1])
		
		self._raba_namespace = namespace
		self.connection = RabaConnection(self._raba_namespace)
		try :
			tableName = self._makeTableName(argk['indexedClass'], argk['relationName'], argk['anchorObj']._rabaClass)
			cur = self.connection.cursor()
			if not self.connection.tableExits(tableName) :
				#cur.execute('CREATE TABLE %s(anchorId, raba_id, PRIMARY KEY(anchorId, raba_id))' % tableName)
				self.connection.createTable(tableName, 'anchorId, raba_id, PRIMARY KEY(anchorId, raba_id)')
				
				self.connection.registerRabalist(argk['anchorObj']._rabaClass, argk['relationName'], argk['indexedClass'], tableName)

			cur = self.connection.cursor()
			cur.execute('SELECT * FROM %s WHERE anchorId = ?' % tableName, (argk['anchorObj'].raba_id, ))
			for aidi in cur :
				self.append(RabaPupa(argk['indexedClass'], aidi[1]))
				
		except (KeyError, sq.OperationalError) :
			pass
			
	def extend(self, v) :
		check = self._checkRabaList(v)
		if not check[0]:
			self._dieInvalidRaba(check[1])
		list.extend(self, v)			
	
	def append(self, v) :
		if not self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.append(self, v)

	def insert(self, k, v) :
		if not self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.insert(self, k, v)
	
	def pupatizeElements(self) :
		"""Transform all raba object into pupas"""
		for i in range(len(self)) :
			self[i] = self[i].pupa()

	def _erase(self, relationName , anchorObj) :
		tableName = self._makeTableName(self[0]._rabaClass, relationName, anchorObj._rabaClass)
		cur = self.connection.cursor()
		cur.execute('UPDATE %s SET anchorId = NULL, raba_id = NULL WHERE anchorId = ?' % tableName, (anchorObj.raba_id,))
	
	def _save(self, relationName , anchorObj) :
		"""saves the RabaList into it's own table. This a private function that should be called directly
		Before saving the entire list corresponding to the anchorObj is wiped out before being rewritten. The
		alternative would be to keep the sync between the list and the table in real time (remove in both).
		If the current solution proves to be to slow, i'll consider the alternative"""
		
		if len(self) > 0 :
			
			self._erase(relationName , anchorObj)
			
			values = []
			for e in self :
				e.save()
				values.append((anchorObj.raba_id, e.raba_id))
			
			tableName = self._makeTableName(self[0].__class__, relationName, anchorObj._rabaClass)
			self.connection.cursor().executemany('INSERT INTO %s (anchorId, raba_id) VALUES (?, ?)' % tableName, values)
			self.connection.commit()

	def _makeTableName(self, indexedClass, relationName, anchorClass) :
		return 'RabaList_%s_type_%s_in_%s' % (relationName, indexedClass.__name__, anchorClass.__name__)
		
	def __setitem__(self, k, v) :
		if self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)

	def __repr__(self) :
		return '<RL'+list.__repr__(self)+'>'
