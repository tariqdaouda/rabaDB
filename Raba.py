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
	return hasattr(v, '__class__') and hasattr(v.__class__, '_isRabaList') and v.__class__._isRabaList

def isRabaType(v) :
	return hasattr(v, '__class__') and hasattr(v.__class__, '_isRabaType') and v.__class__._isRabaType

def isRabaClass(v) :
	return hasattr(v, '__class__') and hasattr(v.__class__, '_isRabaClass') and v.__class__._isRabaClass
	
def isPrimitiveType(v) :
	primTypes = [types.IntType, types.LongType, types.FloatType, types.StringType, types.UnicodeType, types.BufferType, types.NoneType]
	for t in primTypes :
		if isinstance(v, t) : 
			return True
	return False

class Autoincrement :
	def __new__(cls, name, bases, dct) :
		return cls

class _Raba_MetaClass(type) :
	def __new__(cls, name, bases, dct) :
		fields = []
		autoIncr = True
		
		if 'id' not in dct :
			dct['id'] = Autoincrement
			
		for k, v in dct.items():
			if k[0] != '_' and k != 'id' :
				fields.append(k)
		
		if dct['id'] is Autoincrement :
			idStr = 'id INTEGER PRIMARY KEY AUTOINCREMENT'
		else :
			idStr = 'id PRIMARY KEY'
		
		con = RabaConnection()
		
		if name != 'Raba' and not con.tableExits(name) :
			if len(fields) > 0 :
				sql = 'CREATE TABLE %s (%s, %s)' % (name, idStr, ', '.join(list(fields)))
			else :
				sql = 'CREATE TABLE %s (%s)' % (name, idStr)
			
			con.cursor().execute(sql)
			con.connection.commit()
			
		
		return type.__new__(cls, name, bases, dct)

class RabaType(object) :
	_isRabaType = True
	
	def __init__(self, classObj) :
		if not isRabaClass(classObj) :
			self.classObj = classObj
		else :
			raise TypeError('%s is not a valid Raba type (subclass of raba)' % classObj)
			
class RabaPupa(object) :
	"""One of the founding principles of RabaDB is to separate the storage from the code. Fields are stored in the DB while the processing only depends
	on your python code. This approach ensures a higher degree of stability by preventing old objects from lurking inside the DB before popping out of nowhere several decades afterwards. 
	According to this apparoach, raba objects are not serialised but transformed into pupas before being stored. A pupa is a very light object that contains only a reference
	to the raba object class, and it's unique id. Upon asking for one of the attributes of a pupa, it magically transforms into a full fledged raba object. This process is completly transparent to the user. Pupas also have the advantage of being light weight and also ensure that the only raba objects loaded are those explicitely accessed, thus potentialy saving a lot of memory.
	For a pupa self._rabaClass refers to the class of the object "inside" the pupa.
	"""
	_isRabaClass = True

	def __init__(self, classObj, uniqueId) :
		self._rabaClass = classObj
		self.id = uniqueId
		self.__doc__ = classObj.__doc__
		self.bypassMutationAttr = set(['_isRabaClass', '_rabaClass', 'id', '__class__', '__doc__'])
			
	def __getattribute__(self, name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
	
		if name in getAttr('bypassMutationAttr'):
			return object.__getattribute__(self, name)
		
		setAttr('__class__', getAttr('_rabaClass'))
		uniqueId = getAttr('id')
		
		purge = getAttr('__dict__').keys()
		for k in purge :
			delattr(self, k)
			
		Raba.__init__(self, uniqueId)
		
		return object.__getattribute__(self, name)
	
	def __repr__(self) :
		return "<Raba pupa: %s, id %s>" % (self._rabaClass.__name__, self.id)

class Raba(object):
	"All raba object must inherit from this class. If the class has no attribute id, an autoincrement field id will be created"	
	__metaclass__ = _Raba_MetaClass
	_isRabaClass = True
	
	def __init__(self, uniqueId = None) :
		
		if self.__class__ == Raba :
			raise TypeError('Raba class should never be instanciated, use inheritance')

		self._rabaClass = self.__class__
			
		self.id = uniqueId
		if uniqueId != None and uniqueId != '' :
			self._idIsSet = True
		else :
			self._idIsSet = False
			
		if  not  hasattr(self.__class__, 'id') :
			setattr(self.__class__, 'id', Autoincrement)
		
		self.connection = RabaConnection()
		self.columns = {}
		cur = self.connection.cursor()
		col = cur.execute('PRAGMA table_info(%s)' % self.__class__.__name__ )
		
		for c in col.fetchall() :
			if c[1] != 'id' and c[1] not in self.__class__.__dict__:
				cur.execute('UPDATE %s SET %s=NULL WHERE 1;' % (self.__class__.__name__ , c[1]))
			else :
				self.columns[c[0]] = c[1]
		
		self.connection.commit()
		
		if self._idIsSet :
			sql = ('SELECT * FROM %s WHERE id = ?' % self.__class__.__name__)
			cur = self.connection.cursor()
			res = cur.execute(sql, (uniqueId, )).fetchone()
			if res != None :
				self._newEntry = False
				for i in self.columns :
					if self.columns[i] != 'id' :
						elmt = getattr(self.__class__, self.columns[i])
						if isPrimitiveType(elmt) :
							self.__setattr__(self.columns[i], res[i])
						elif isRabaType(elmt) :
							if not isinstance(res[i], types.NoneType) :
								li = RabaListPupa(indexedClass = elmt.classObj, relationName = self.columns[i], anchorObj = self)
								self.__setattr__(self.columns[i], li)
						else :
							if res[i] != None :
								self.__setattr__(self.columns[i], cPickle.loads(str(res[i])))
							
			else :
				self._newEntry = True
		else :
			self._newEntry = True
		
	def autoclean(self) :
		"""TODO: Copies the table into a new one removing all the collumns that have all their values to NULL
		and drop the tables that correspond to these tables"""
		pass
	
	def pupa(self) :
		"""returns a pupa version of self"""
		return RabaPupa(self.__class__, self.id)
		
	def save(self) :
		fields = []
		values = []
		rabalists = []
		cur = self.connection.cursor()
		for k, v in self.__class__.__dict__.items() :
			if k in self.__dict__ :
				val = self.__dict__[k]
			else :
				val = v
			
			if not isinstance(val, types.FunctionType) and k[0] != '_'  and k != 'id' :
				if k not in self.columns.values() :
					sql = 'ALTER TABLE %s ADD %s;' % (self.__class__.__name__, k)
					self.connection.cursor().execute(sql)
				
				fields.append(k)
				if isPrimitiveType(val) :
					values.append(val)
				elif isRabaList(val) :
					rabalists.append((k, val))
					values.append('~rabalist~')
				else :
					#serialize
					values.append(buffer(cPickle.dumps(val)))
					
		if len(values) > 0 :
			if self._newEntry :
				questionMarks = []
				if self.__class__.id is Autoincrement and not self._idIsSet :
					for i in range(len(values)) :
						questionMarks.append('?')
					sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ','.join(fields), ','.join(questionMarks))
					cur.execute(sql, values)
					self.id = cur.lastrowid
					self._idIsSet = True
				else :
					
					if self.id == None or self.id == '' :
						raise ValueError("I can't save a RabaObject (%s) whose id value is None or ''" % self)
					
					fields.append('id')
					values.append(self.id)
					for i in range(len(values)) :
						questionMarks.append('?')
					sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ','.join(fields), ','.join(questionMarks))
					cur.execute(sql, values)
			else :
				sql = 'UPDATE %s SET %s = ? WHERE id = ?' % (self.__class__.__name__, ' = ?, '.join(fields))
				values.append(self.id)
				cur.execute(sql, values)
		else :
			raise ValueError('class %s has no fields to save' % self.__class__.__name__)
		
		for relation, l in rabalists :
			l._save(relation, self)
			
		self.connection.commit()

	def __setattr__(self, k, v) :
		if k == 'id' and k in self.__dict__ and self.__dict__[k] != None :
			raise KeyError("You cannot change the id once it has been set ( %s => %s, obj: %s)." % (self.id, v, self))
		elif hasattr(self.__class__, k) and isRabaType(getattr(self.__class__, k)) and not isRabaList(v) : #and not isRabaClass(v) 
			raise TypeError("I'm sorry but you can't replace a raba type by someting else (%s: from %s to %s)" %(k, getattr(self.__class__, k), v))
		else :
			object.__setattr__(self, k, v)
	
	def __getattribute__(self, k) :
		elmt = object.__getattribute__(self, k)
		
		#transform into a RabaListPupa
		if isRabaType(elmt) :
			elmt = RabaListPupa(indexedClass = elmt.classObj, relationName = k, anchorObj = self)
			setattr(self, k, elmt)
		
		return elmt
		
	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __hash__(self) :
		return self.__class__.__name__+str(self.uniqueId)
	
	def __repr__(self) :
		return "<Raba obj: %s, id %s>" % (self._rabaClass.__name__, self.id)
	
class RabaListPupa(list) :
	_isRabaList = True
	
	def __init__(self, indexedClass, relationName, anchorObj) :
		self.relationName = relationName
		self.anchorObj = anchorObj
		self.indexedClass = indexedClass
		self.bypassMutationAttr = set(['relationName', 'anchorObj', 'indexedClass', '__class__'])
		
	def _morph(self) :
		def getAttr(name) :
			return list.__getattribute__(self, name)
			
		def setAttr(name, value) :
			list.__setattr__(self, name, value)
		
		setAttr('__class__', RabaList)
		
		indC = getAttr('indexedClass')
		relName = getAttr('relationName')
		anchObj = getAttr('anchorObj')
		
		purge = getAttr('__dict__').keys()
		for k in purge :
			delattr(self, k)
		
		RabaList.__init__(self, indexedClass = indC, relationName = relName, anchorObj = anchObj)
		
	def __getitem__(self, k) :
		self._morph()
		#print 'll', self
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
			
		if len(self) > 0 and v._rabaClass != self[0]._rabaClass :
			return False
		
		return True
		
	def _checkRabaList(self, v) :
		vv = list(v)
		for e in vv :
			if not self._checkElmt(e) :
				return (False, e)
		return (True, None)
	
	def _dieInvalidRaba(self, v) :
		raise TypeError('Only Raba objects of the same class can be stored in RabaLists. Elmt: %s is not a valid RabaObject' % v)
			
	def __init__(self, *argv, **argk) :
		list.__init__(self, *argv)
		check = self._checkRabaList(self)
		if not check[0]:
			self._dieInvalidRaba(check[1])
		
		self.connection = RabaConnection()
		try :
			tableName = self._makeTableName(argk['indexedClass'], argk['relationName'], argk['anchorObj'])
			cur = self.connection.cursor()
			cur.execute('SELECT * FROM %s WHERE 1' % tableName)
			for aidi in cur :
				self.append(RabaPupa(argk['indexedClass'], aidi[0]))
				
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

	def _save(self, relationName , anchorObj) :
		"""saves the RabaList into it's own table. This a private function that should be called directly"""
		if len(self) > 0 :
			tableName = self._makeTableName(self[0]._rabaClass, relationName, anchorObj)
		
			cur = self.connection.cursor()
			cur.execute('DROP TABLE IF EXISTS %s' % tableName)
			cur.execute('CREATE TABLE %s(id)' % tableName)
			values = []
			for e in self :
				e.save()
				values.append((e.id, ))
				
			cur.executemany('INSERT INTO %s (id) VALUES (?)' % tableName, values)
			self.connection.commit()

	def _makeTableName(self, indexedClass, relationName, anchorObj) :
		return 'RabaList_%s_of_%s_BelongsTo_%s_id_%s' % (relationName, indexedClass.__name__, anchorObj.__class__.__name__, anchorObj.id)
		
	def __setitem__(self, k, v) :
		if self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)

	def __repr__(self) :
		return '<RL'+list.__repr__(self)+'>'
