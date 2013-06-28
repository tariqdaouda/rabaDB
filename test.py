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

class A(object) :
	def __init__(self) :
		self.f = 'F de A'
		self.g = {'map':'tralalal'}
	
	def __getattr__(self, name) :
		return self.g[name]
		
	def __propagateRaba(self) :
		for k, v in self.__dict__.items() :
			if k[0] != '_' :
				if type(v).__name__  in ['str', 'int', 'float'] :
					return v
				elif hasattr(v, '_rabaObject') and v._rabaObject :
					return v.getPuppa()
				else :
					return cPickle.dump(v)

	def op(self) :
		return 'op'
		
class B(object) :
	def __init__(self) :
		self.f = 'p'
	
	def a(self) :
		print 'we'
		
	def __getattribute__(self,name) :
		def setField(name, value) :
			object.__setattr__(self, name, value)
		#print dir(self)
 		setField('__class__', A)
		A.__init__(self)
		return object.__getattribute__(self, name)

class Raba(object) :
	_rabaObject = True

class RabaCollection(list) :
	def _checkRabaElmt(self, v) :
		if hasattr(v, '_rabaObject') and v._rabaObject == True :
			return True
		return False
	
	def _checkRabaList(self, v) :
		vv = list(v)
		for e in vv :
			if not self._checkRabaElmt(e) :
				return (False, e)
		return (True, None)
	
	def _dieInvalidRaba(self, v) :
		raise TypeError('Only RabaObjects can be stored in RabaCollections. Elmt: %s is not a valid RabaObject' % v)
			
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
		if not self._checkRabaElmt(v) :
			self._dieInvalidRaba(v)
		list.append(self, v)

	def insert(self, k, v) :
		if not self._checkRabaElmt(v) :
			self._dieInvalidRaba(v)
		list.insert(self, k, v)
		
	def __setitem__(self, k, v) :
		if self._checkRabaElmt(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)

#print RabaCollection( (Raba(),Raba(), 3) )
rc = RabaCollection( (Raba(),Raba()) )
rc.append(Raba())
rc.extend( set([Raba()]) )
print rc
rc.insert(0, Raba())
print rc.pop()
#rc.append('3')
#rc[0] = '3'
#print rc


#r = Raba()
#print r.__dict__
#print r.__getattribute__, r.pupa
	
#b = B()
#print b, b.f, b.__class__, b.op()
#print b.map
#b.op()
