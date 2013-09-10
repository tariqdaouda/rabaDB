import types

RABA_FIELD_TYPE_IS_UNDEFINED = 'UNDEFINED'
RABA_FIELD_TYPE_IS_PRIMITIVE = 'PRIMITIVE'
RABA_FIELD_TYPE_IS_RABA_OBJECT = 'RABA_OBJECT'

class RabaField(object) :
	_raba_field = True
	_raba_list = False
	_raba_type = RABA_FIELD_TYPE_IS_UNDEFINED
	
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		if self.__class__ == 'RabaField' :
			raise ValueError("RabaField is abstract and should not be instanciated, Instanciate one of it's children")
		
		self.default = default
		self.constrainFct = constrainFct
		self.constrainFctArgs = constrainFctArgs
		self.constrainFctWArgs = constrainFctWArgs
		
	def check(self, val) :
		if self.constrainFct == None :
			return True
		
		return self.constrainFct(val, *self.constrainFctArgs, **self.constrainFctWArgs)

class List(object) :
	_raba_field = True
	_raba_list = True

	def __init__(self) :
		self.default = None
	
class Primitive(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_PRIMITIVE
	
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		RabaField.__init__(self,  default, constrainFct, *constrainFctArgs, **constrainFctWArgs)
	
	def check(self, val) :
		return RabaField.check(self, val)
	
	def __repr__(self) :
		return '<field %s, default: %s>' % (self.__class__.__name__, self.default)
		
class RabaObject(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_RABA_OBJECT
	
	def __init__(self, objClassName = None, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		
		if default != None and not isRabaObject(default) :
			raise ValueError("Defailt is not a valid Raba Object")
		
		RabaField.__init__(self,  default, constrainFct, *constrainFctArgs, **constrainFctWArgs)
		self.objClassName = objClassName
		
	def check(self, val) :
		if val == self.default and self.default == None :
			return True
		return isRabaObject(val) and ((self.objClassName != None and val._rabaClass.__name__ == self.objClassName) or self.objClassName == None) and RabaField.check(self, val)

	def __repr__(self) :
		return '<field %s, class: %s , default: %s>' % (self.__class__.__name__, self.objClassName, self.default)

def isRabaObject(v) :
	return hasattr(v, '_rabaClass')
	
def isField(v) :
	return hasattr(v.__class__, '_raba_field') and v.__class__._raba_field
	
def isRabaList(v) :
	return hasattr(v.__class__, '_raba_list') and v.__class__._raba_list

def typeIsPrimitive(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_PRIMITIVE

def typeIsRabaObject(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_RABA_OBJECT

def isPythonPrimitive(v) :
	primTypes = [types.IntType, types.LongType, types.FloatType, types.StringType, types.UnicodeType, types.BufferType, types.NoneType]
	for t in primTypes :
		if isinstance(v, t) : 
			return True
	return False
