import cPickle

class Raba(object):
	__rabaObject = True
	
	def __init__(self, uniqueId = None) :
		self._pupa = True
		self.uniqueId = uniqueId
		
	def load(self, uniqueId = None) :
		def getField(name) :
			return object.__getattribute__(self, name)
		
		def setField(name, value) :
			object.__setattr__(self, name, value)
			
		if getField('_pupa') :
			fields = {}
			fields['a'] = 'aa'
			setField('fields', fields)
			setField('pupa', False)

	def getPuppa(self) :
		return self.__class__(self.uniqueId)
		
	def __getitem__(self, k) :
		return self.fields[k]

	def __setitem(self, k, v) :
		self.fields[k] = v

	def __getattribute__(self, name):
		if object.__getattribute__(self, '_pupa') :
			object.__getattribute__(self, 'load')()
		return object.__getattribute__(self, name)

class A :
	def __init__(self) :
		pass
	
	def __propagateRaba(self) :
		for k, v in self.__dict__.items() :
			if k[0] != '_' :
				if type(v).__name__  in ['str', 'int', 'float'] :
					return v
				elif hasattr(v, '_rabaObject') and v._rabaObject :
					return v.getPuppa()
				else :
					return cPickle.dump(v)
					
r = Raba()
print r.__dict__
print r.__getattribute__, r.pupa
