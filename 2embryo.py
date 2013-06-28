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
	
	def __init__(self, classObj, uniqueId) :
		self.classObj = classObj
		self.uniqueId = uniqueId
	
	def __getattribute__(self,name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
	
		setAttr('__class__', getAttr('classObj'))
		A.__init__(self, getAttr('uniqueId'))
		
		return object.__getattribute__(self, name)

def isRabaCollection(e) :
	return e.__class__.__name__ == 'RabaCollection'

def isRabaObject(e) :
	return hasattr(v, '_rabaObject') and v._rabaObject == True and e.__class__.__name__ != 'Raba'
	
class RabaCollection(list) :
	"""A RabaCollection is a list that can only contain Raba objects"""
	
	def _checkRabaList(self, v) :
		vv = list(v)
		for e in vv :
			if not isRabaObject(e) :
				return (False, e)
		return (True, None)
	
	def _dieInvalidRaba(self, v) :
		raise TypeError('Only Raba objects can be stored in RabaCollections. Elmt: %s is not a valid RabaObject' % v)
			
	def __init__(self, *argv, **argk) :
		list.__init__(self, *argv, **argk)
		check = self._checkRabaList(self)
		if not check[0]:
			self._dieInvalidRaba(check[1])
	
		
	def extend(self, v) :
		check = self._checkRabaList(v)
		if not check[0]:
			self._dieInvalidRaba(check[1])
		list.extend(self, v)			
	
	def append(self, v) :
		if not isRabaObject(v) :
			self._dieInvalidRaba(v)
		list.append(self, v)

	def insert(self, k, v) :
		if not isRabaObject(v) :
			self._dieInvalidRaba(v)
		list.insert(self, k, v)
		
	def __setitem__(self, k, v) :
		if isRabaObject(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)
			
class Raba(object):
	__metaclass__ = _Raba_MetaClass
	_rabaObject = True
	
	def __init__(self, uniqueId = None) :
		self.columns = {}
		col = RABA_CONNECTION.cursor().execute('PRAGMA table_info(?)', (self.__class__.__name__, ))
		for c in col :
			self.columns[c[0]] = c[1]
			
		if uniqueId != None :
			sql = ('SELECT * FROM %s WHERE id = ?' % self.__class__.__name__)
			cur = RABA_CONNECTION.cursor()
			
			for row in cur.execute(sql, (uniqueId, ) ).fetchone() :
				for i in range(len(row)) :
					if hasattr(self, self.columns[i]) :
						elmt = self.__getattribute__(self.columns[i])
						if isinstance(elmt, types.ListType) :
							elmt.append(row[i])
						elif isRabaCollection(elmt) :
							elmt.append(RabaPupa(self.columns[i], row[i]))
						elif isRabatObject() :
							elmt = RabaCollection([elmt])
							elmt.append(RabaPupa(self.columns[i], row[i]))
							self.__setattr__(columns[i], elmt)
						else :
							elmt = [elmt]
							self.__setattr__(columns[i], elmt)
				
	def save(self) :
		pass

	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __hash__(self) :
		return self.__class__.__name__+str(self.uniqueId)
		
class Chromosome(Raba) :
	#genes = RabaObjectList()
	name = 'symb'
	x1 = 10
	x2 = 100
	def __init__(self) :
		pass

autoremove()
