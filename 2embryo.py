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
	
class __Raba_MetaClass(type) :
	def __new__(cls, name, bases, dct) :		
		fields = []
		#exceptFields = set(('__module__', '__metaclass__', '__rabaObject'))
		#primitiveTypes = ['str', 'int', 'float']
		for k, v in dct.items():
			#if k.find('__') != 0 and  or type(v).__name__ in primitiveTypes :
			if k[0] != '_' :
				fields.append(k)
		
		if name != 'Raba' :
			conf.updateType(name, fields)
		
		return type.__new__(cls, name, bases, dct)

class Raba(object):
	__metaclass__ = __Raba_MetaClass
	_rabaObject = True
	
	def __init__(self, uniqueId = None) :
		self._pupa = True

	def load(self, uniqueId = None) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
		
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
			
		if getAttr('_pupa') : :
			fields = {}
			columns = {}
			col = RABA_CONNECTION.cursor().execute('PRAGMA table_info(%s)' % self.__class__.__name__)
			for c in col :
				fields[c[1]] = None
				columns[c[0]] = c[1]
				
			if uniqueId != None :
				sql = ('SELECT * FROM %s WHERE id = ?' % self.__class__.__name__)
				cur = RABA_CONNECTION.cursor()
				row = cur.execute(sql, uniqueId).fetchone()
				for i in range(len(row)) :
					self.fields[columns[i]] = row[i]
					
			setField('fields', fields)
			setField('pupa', False)
		
	def __propagateRaba(self) :
		for k, v in self.__dict__.items() :
			if k[:2] != '__' :
				if hasattr(v, '__iter__') :
					
	def save(self) :
		pass

	def __getitem__(self, k) :
		return self.fields[k]

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __getattribute__(self, name):
		if self.__pupa :
			self.load()
		return object.__getattribute__(self, name)
		
class Chromosome(Raba) :
	#genes = RabaObjectList()
	name = 'symb'
	x1 = 10
	x2 = 100
	def __init__(self) :
		pass

autoremove()
