import sqlite3 as sq
import os, types
import setup as conf

def autoremove() :
	cl = set()
	for c in Raba.__subclasses__() :
		cl.add(c.__name__)
	
	toRemove = []
	for c in conf.TYPES :
		if c not in cl :
			toRemove.append(c)
	
	for c in toRemove :
		conf.removeType(c)

def isRabaList(e) :
	return e.__class__ == RabaList

def _isRabaType(e) :
	return hasattr(v, '_rabaType') and v._rabaType == True
	
class _Raba_MetaClass(type) :
	def __new__(cls, name, bases, dct) :		
		fields = []
		for k, v in dct.items():
			if k[0] != '_' :
				fields.append(k)
		
		if name != 'Raba' :
			conf.updateType(name, fields)
		
		return type.__new__(cls, name, bases, dct)

class RabaPupa(object) :
	"""One of the founding principles of RabaDB is to separate the storage from the code. Fields are stored in the DB while the processing only depends
	on your python code. This approach ensures a higher degree of stability by preventing old objects from lurking inside the DB before popping out of nowhere several decades afterwards. 
	According to this apparoach, raba objects are not serialised but transformed into pupas before being stored. A pupa is a very light object that contains only a reference
	to the raba object class, and it's unique id. Upon asking for one of the attributes of a pupa, it magically transforms into a full fledged raba object. This process is completly transparent to the user. Pupas also have the advantage of being light weight and also ensure that the only raba objects loaded are those explicitely accessed, thus potentialy saving a lot of memory.
	"""
	
	_rabaType = True
	
	def __init__(self, classObj, uniqueId) :
		self.classObj = classObj
		self.uniqueId = uniqueId
	
	def __getattribute__(self,name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
	
		setAttr('__class__', getAttr('classObj'))
		Raba.__init__(self, getAttr('uniqueId'))
		
		return object.__getattribute__(self, name)
		
class Raba(object):
	
	__metaclass__ = _Raba_MetaClass
	_rabaType = True
	
	def __init__(self, uniqueId = None) :
		if self.__class__ == Raba :
			raise TypeError('Raba class should never be instanciated, use inheritance')
			
		self.columns = {}
		col = conf.RABA_CONNECTION.cursor().execute('PRAGMA table_info(%s)' % self.__class__.__name__ )
		
		for c in col :
			if c[1] not in self.__dict__.values() and c[1] not in self.__class__.__dict__.values()
				cur.execute('UPDATE Relations SET %s=NULL WHERE 1;' % c[1])
			else :
				self.columns[c[0]] = c[1]
		RABA_CONNECTION.commit()
		
		self.id =  uniqueId	
		if uniqueId != None :
			sql = ('SELECT * FROM %s WHERE id = ?' % self.__class__.__name__)
			cur = conf.RABA_CONNECTION.cursor()
			cur.execute(sql, (uniqueId, ))
			for row in cur :
				for i in range(len(row)) :
					if hasattr(self, self.columns[i]) :
						if isinstance(elmt, types.IntType) or isinstance(elmt, types.FloatType) or isinstance(elmt, types.FloatType) :
							self.__setattr__(columns[i], row[i])
						if isRabaList(elmt) :
							self.__setattr__(columns[i], RabaListPupa(self.columns[i]#, row[i])
						if isRabatObject() :
							self.__setattr__(columns[i], RabaPupa(self.columns[i], row[i])
						else :
							self.__setattr__(columns[i], LOAD SERIALISATIOn)
		
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
		cur = RABA_CONNECTION.cursor()
		for k, v in self.__dict__.items() :
			if isRabaType(v) : #isinstance(v, types.FunctionType) :
				cur.execute('INSERT INTO % (%s) VALUES ?' % (self.__class__.__name__, k), (k.id, ))
			if isRabaList(v) :
				v._save(self.__class__)
				cur.execute('INSERT INTO % (%s) VALUES ?' % (self.__class__.__name__, k), (k.tableName, ))
				
				
		if self.id == None :
			sql = 'INSERT INTO %s ()'

	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __hash__(self) :
		return self.__class__.__name__+str(self.uniqueId)

class RabaListPupa(object) :
	
	def __init__(self, relationName, anchorObj, elmtsClassObj) :
		self.relationName = relationName
		self.anchorObj = anchorObj
		self.elmtsClassObj = elmtsClassObj
	
	def __getattribute__(self,name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
	
		setAttr('__class__', getAttr('classObj'))
		RabaList.__init__(self, getAttr('relationName'), getAttr('anchorObj'), getAttr('elmtsClassObj'))
		
		return object.__getattribute__(self, name)

class RabaList(list) :
	"""A RabaList is a list that can only contain Raba objects of the same class or (Pupas of the same class). They represent one to many relations and are stored in separate
	tables that contain only one single line"""
	
	def _checkElmt(self, v) :
		if not _isRabaType(v) :
			return False
			
		if len(self) > 0 and v.__class__ != self[0].__class__ and (v.__class__ != RabaPupa or v.elmtsClassObj != self[0].__class__) :
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
		list.__init__(self, *argv, **argk)
		check = self._checkRabaList(self)
		if not check[0]:
			self._dieInvalidRaba(check[1])
			
		try :
			self.elmtsClassObj = argk['elmtsClassObj']
			tableName = self._makeTableName(argk['relationName'], argk['anchorObj'])
			cur = RABA_CONNECTION.cursor()
			cur.execute('SELECT * FROM %s' % tableName)
			for aidi in cur :
				self.append(RabaPupa(self.elmtsClassObj, aidi[0]))
				
		except KeyError:
			self.elmtsClassObj = None
			
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
			tableName = self._makeTableName(relationName , anchorObj)
		
			cur = conf.RABA_CONNECTION.cursor()
			cur.execute('DROP TABLE IF EXITS %s' % tableName)
			cur.execute('CREATE TABLE %s(id)' % tableName)
			values = []
			for e in self :
				values.append((e.id, ))
			cur.executemany('INSET INTO %s (id) VALUES (?)' % tableName, values)
			RABA_CONNECTION.commit()

	def _makeTableName(self, relationName , anchorObj) :
		"#ex: RabaList_non-synsnps(snps)_of_gene_ENSG"
		return 'RabaList_%s(%s)_of_%s_%s' % (relationName, self.elmtsClass, anchorObj.__class__.__name__, anchorObj.id)
		
	def __setitem__(self, k, v) :
		if self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)

	
class Chromosome(Raba) :
	#genes = RabaObjectList()
	name = 'symb'
	x1 = 10
	x2 = 100
	def __init__(self) :
		Raba.__init__(self)

autoremove()

c = Chromosome()
