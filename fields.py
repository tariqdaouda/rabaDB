import types
import Raba

RABA_FIELD_TYPE_IS_UNDEFINED = 'UNDEFINED'
RABA_FIELD_TYPE_IS_PRIMITIVE = 'PRIMITIVE'
RABA_FIELD_TYPE_IS_RABA_OBJECT = 'RABA_OBJECT'
RABA_FIELD_TYPE_IS_RABA_LIST = 'RABA_LIST'

class RabaField(object) :
	_raba_field = True
	_raba_list = False
	_raba_type = RABA_FIELD_TYPE_IS_UNDEFINED
	
	def __init__(self, default = None, constrainFct = None, **constrainFctWArgs) :
		if self.__class__ == 'RabaField' :
			raise ValueError("RabaField is abstract and should not be instanciated, Instanciate one of it's children")
		
		self.default = default
		self.constrainFct = constrainFct
		self.constrainFctWArgs = constrainFctWArgs
		
	def check(self, val) :
		if self.constrainFct == None :
			return True
		
		return self.constrainFct(val, **self.constrainFctWArgs)

class RabaListField(RabaField) :
	_raba_field = True
	_raba_list = True
	_raba_type = RABA_FIELD_TYPE_IS_RABA_LIST

	def __init__(self, ElmtConstrainFct = None, **ElmtConstrainFctWArgs) :
		RabaField.__init__(self, default = None, constrainFct = ElmtConstrainFct, **ElmtConstrainFctWArgs)
		del(self.default)# = None
		
class PrimitiveField(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_PRIMITIVE
	
	def __init__(self, default = None, constrainFct = None, **constrainFctWArgs) :
		RabaField.__init__(self,  default, constrainFct, **constrainFctWArgs)
	
	def check(self, val) :
		return RabaField.check(self, val)
	
	def __repr__(self) :
		return '<field %s, default: %s>' % (self.__class__.__name__, self.default)
		
class RabaObjectField(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_RABA_OBJECT
	
	def __init__(self, objClassName = None, default = None, constrainFct = None, **constrainFctWArgs) :
		
		if default != None and not isRabaObject(default) :
			raise ValueError("Default value is not a valid Raba Object")
		
		RabaField.__init__(self,  default, constrainFct, **constrainFctWArgs)
		self.objClassName = objClassName
		
	def check(self, val) :
		if val == self.default and self.default == None :
			return True
		return Raba.isRabaObject(val) and ((self.objClassName != None and val._rabaClass.__name__ == self.objClassName) or self.objClassName == None) and RabaField.check(self, val)

	def __repr__(self) :
		return '<field %s, class: %s , default: %s>' % (self.__class__.__name__, self.objClassName, self.default)

	
def isField(v) :
	return hasattr(v.__class__, '_raba_field') and v.__class__._raba_field

def typeIsPrimitive(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_PRIMITIVE

def typeIsRabaObject(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_RABA_OBJECT

def typeIsRabaList(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_RABA_LIST

