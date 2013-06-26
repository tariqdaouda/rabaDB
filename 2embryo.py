import sqlite3 as sq
import os
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

_RABA_OBJECT = 0
_RABA_PRIMITIVE = 1
_RABA_DICTIONARY = 2

class RabaDictionary(dict) :
	_rabaidentity_ = _RABA_DICTIONARY
	def __setitem__(self, k, v) :
		if hasattr(k, '_rabaidentity_') and k._rabaidentity_ == _RABA_OBJECT:
			dict.__setitem__(k, v)
		else :
			raise TypeError('Only subclasses of Raba can be added to a RabaDictionary')

class RabaField(object) :
	def __new__(cls, *args, **kwargs) :
		class RabaPrimitive(args[0].__class__) :
			_rabaidentity_ = _RABA_PRIMITIVE
		
		return RabaPrimitive(*args, **kwargs)
		
class Raba_MetaClass(type) :
	def __new__(cls, name, bases, dct) :
		
		fields = []
		exceptFields = set(('__module__', '__metaclass__', '_rabaidentity_'))
		for k, v in dct.items():
			if hasattr(v, '_rabaidentity_') and v._rabaidentity_ == _RABA_PRIMITIVE :
				fields.append(k)
		
		if not conf.hasType(name) and name != 'Raba' :
			conf.addType(name, fields)
		
		return type.__new__(cls, name, bases, dct)

class __NotInstanciated :
	__metaclass__ = type
	def __init__(self, uniqueId, classType) :
		self.uniqueId = uniqueId
		self.classType = classType
		
class Raba(object):
	__metaclass__ = Raba_MetaClass
	_rabaidentity_ = _RABA_OBJECT
	
	def __init__(self, uniqueId = None) :
		self.load(uniqueId)

	def load(self, uniqueId = None) :
		self.fields = {}
		self.values = {}
		col = RABA_CONNECTION.cursor().execute('PRAGMA table_info(%s)' % self.__class__.__name__)
		for c in col :
			self.fields[c[0]] = c[1]
			self.values[c[1]] = None
			
		if uniqueId != None :
			sql = ('SELECT * FROM %s WHERE id = ?' % self.__class__.__name__)
			cur = RABA_CONNECTION.cursor()
			row = cur.execute(sql, uniqueId).fetchone()
			for i in range(len(row)) :
				self.values[self.fields[i]] = row[i]
	
	def __propagateRaba(self) :
		pass
		
	def save(self) :
		pass

	def __getitem__(self, k) :
		return self.fields[k]

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __getattribute__(self, name):
		obj = object.__getattribute__(self, name)
		if obj.__class__.__name__ == '__NotInstanciated' :
			obj = obj.classType(obj.uniqueId)
			object.__setattr__(self, name, obj)
		return obj

class Chromosome(Raba) :
	#genes = RabaObjectList()
	length = RabaField(10)
	x1 = RabaField('100')
	x2 = 100
	def __init__(self) :
		pass

autoremove()
