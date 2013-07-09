import sqlite3 as sq
import os, types, cPickle
from setup import RabaConnection


def getClassTYPES() :
	"Returns the sub classes of Raba that have been imported. Warning if classes have not been imported, there's no way for python to know about them"
	types = set()
	for c in Raba.__subclasses__() :
		types.add(c.__name__)
	return types

def isRabaList(e) :
	return False
	#return e.__class__ == RabaList

def isRabaType(v) :
	return hasattr(v, '__class__') and hasattr(v.__class__, '_rabaType') and v.__class__._rabaType == True

def isRabaClass(v) :
	return hasattr(v, '__class__') and hasattr(v.__class__, '_rabaClass') and v.__class__._rabaClass == True
	
def isPrimitiveType(v) :
	primTypes = [types.IntType, types.LongType, types.FloatType, types.StringType, types.UnicodeType, types.BufferType, types.NoneType]
	for t in primTypes :
		if isinstance(v, t) : 
			return True
	return False

class Autoincrement :
	pass

class _Raba_MetaClass(type) :
	def __new__(cls, name, bases, dct) :		
		fields = []
		autoIncr = True
		
		if 'id' not in dct :
			dct['id'] = Autoincrement
		elif dct['id'].__class__ == Autoincrement :
			dct['id'] = Autoincrement
			
		for k, v in dct.items():
			if k[0] != '_' and k != 'id' :
				fields.append(k)
		
		if dct['id'] ==  Autoincrement :
			idStr = 'id INTEGER PRIMARY KEY AUTOINCREMENT'
		else :
			idStr = 'id PRIMARY KEY'
				
		con = RabaConnection()
		
		if name != 'Raba' and not con.tableExits(name) :
			if len(fields) > 0 :
				sql = 'CREATE TABLE %s (%s, %s)' % (name, idStr, ', '.join(list(fields)))
			else :
				sql = 'CREATE TABLE %s (%s)' % (name, idStr)
			
			#print sql
			con.cursor().execute(sql)
			con.connection.commit()
			
		
		return type.__new__(cls, name, bases, dct)

class RabaType(object) :
	_rabaType = True
	
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
	_rabaClass = True
	
	def __init__(self, classObj, uniqueId) :
		self._rabaClass = classObj
		self.classObj = classObj
		self.uniqueId = uniqueId
	
	def __getattribute__(self, name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
	
		if name  == '__class__' :
			return object.__getattribute__(self, name)
		
		setAttr('__class__', getAttr('classObj'))
		Raba.__init__(self, getAttr('uniqueId'))
		
		return object.__getattribute__(self, name)
		
class Raba(object):
	
	__metaclass__ = _Raba_MetaClass
	_rabaClass = True
	
	def __init__(self, uniqueId = None) :
		"All raba object must inherit from this class. If the class has no attribute id, an autoincrement field id will be created"
		
		self._rabaClass = self.__class__
		
		if self.__class__ == Raba :
			raise TypeError('Raba class should never be instanciated, use inheritance')
		
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
		
		self._idIsSet = False
		if uniqueId != None :
			self.id = uniqueId
			self._idIsSet = True
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
						elif isRabaList(elmt) :
							print "loading rabalist not available yet"
							#self.__setattr__(columns[i], RabaListPupa(self.columns[i]#, res[0][i])
						elif isRabaType(elmt) :
							if not isinstance(res[i], types.NoneType) :
								self.__setattr__(self.columns[i], RabaPupa(elmt.classObj, res[i]))
						else :
							if res[i] != None :
								self.__setattr__(self.columns[i], cPickle.loads(str(res[i])))
							
			else :
				self._newEntry = True
				
		elif hasattr(self.__class__, 'id') :
			self.id = self.__class__.id
			self._newEntry = True
		else :
			self.id = None
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
				if isRabaClass(val) :
					val.save()
					values.append(val.id)
				elif isPrimitiveType(val) :
					values.append(val)
				elif isRabaType(val) :
					#A raba type that has not been instanciated
					values.append(None)
				else :
					#serialize
					values.append(buffer(cPickle.dumps(val)))
					
		if len(values) > 0 :
			if self._newEntry :
				questionMarks = []
				if self.__class__.id == Autoincrement :
					for i in range(len(values)) :
						questionMarks.append('?')
					sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ','.join(fields), ','.join(questionMarks))
					cur.execute(sql, values)
					self.id = cur.lastrowid
					self._idIsSet = True
				else :
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
			
		self.connection.commit()

	def __setattr__(self, k, v) :
		if k == 'id' and self._idIsSet :
			raise KeyError("You cannot change the id once it has been set.")
		elif hasattr(self.__class__, k) and isRabaType(getattr(self.__class__, k)) and not isRabaClass(v) :
			raise TypeError("I'm sorry but you can't replace a raba type by someting else (%s: from %s to %s)" %(k, getattr(self.__class__, k), v))
		else :
			object.__setattr__(self, k, v)
		
	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __hash__(self) :
		return self.__class__.__name__+str(self.uniqueId)

class Gene(Raba) :
	name = ''
	id = None#Autoincrement()
	def __init__(self, name, uniqueId = None) :
		Raba.__init__(self, uniqueId)
		self.name = name
	
class Chromosome(Raba) :
	#genes = RabaObjectList()
	name = None
	x2 = None
	x1 = None
	gene = RabaType(Gene)
	id = None
	alist = []
	def __init__(self, uniqueId = None) :
		Raba.__init__(self, uniqueId)

if __name__ == '__main__' :
	#RabaConnection().dropTable('Gene')
	#RabaConnection().dropTable('Chromosome')
	print "now testing raba types, raba lits later"
	c = Chromosome('22')
	c.x1 = 33
	c.x2 = 5656
	print c.gene.name
	c.gene = Gene('TPST9998', uniqueId = 1)
	print c.alist# = range(10)
	c.save()
