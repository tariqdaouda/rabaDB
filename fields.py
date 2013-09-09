import types

RABA_FIELD_TYPE_IS_UNDEFINED = 'UNDEFINED'
RABA_FIELD_TYPE_IS_PRIMITIVE = 'PRIMITIVE'
#RABA_FIELD_TYPE_IS_SERIALIZED_OBJECT = 'SERIALIZED_OBJECT'
RABA_FIELD_TYPE_IS_RABA_OBJECT = 'RABA_OBJECT'

class RabaField(object) :
	_raba_field = True
	_raba_list = False
	_raba_type = RABA_FIELD_TYPE_IS_UNDEFINED
	
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		self.default = default
		self.constrainFct = constrainFct
		self.constrainFctArgs = constrainFctArgs
		self.constrainFctWArgs = constrainFctWArgs
		
	def check(self, val) :
		if self.constrainFct == None :
			return True
		
		return self.constrainFct(val, *self.constrainFctArgs, **self.constrainFctWArgs)

class RabaFieldList(object) :
	_raba_field = True
	_raba_list = True
	_raba_type = RABA_FIELD_TYPE_IS_UNDEFINED
	
	def __init__(self, rabaFieldClass, default = None, elmtConstrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		self.fieldObj = rabaFieldClass(default, elmtConstrainFct, *constrainFctArgs, **constrainFctWArgs)
	
	def check(self, val) :
		return self.fieldObj.constrainFct(val, *self.constrainFctArgs, **self.constrainFctWArgs)

#types	
class Primitive(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_PRIMITIVE
	
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		RabaField.__init__(self,  default, constrainFct, *constrainFctArgs, **constrainFctWArgs)
	
	def check(self, val) :
		return RabaField.check(self, val)
		#return isPythonPrimitive(val) and RabaField.check(self, val)
	
	def __repr__(self) :
		return '<field %s, default: %s>' % (self.__class__.__name__, self.default)
		
#class SerializedObject(RabaField) :
#	_raba_type = RABA_FIELD_TYPE_IS_SERIALIZED_OBJECT
	
#	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
#		RabaField.__init__(self,  default, constrainFct, *constrainFctArgs, **constrainFctWArgs)
	
class RabaObject(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_RABA_OBJECT
	
	def __init__(self, objClassName = None, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		
		if default != None and not isRabaClass(default) :
			raise ValueError("Defailt is not a valid Raba Object")
		
		RabaField.__init__(self,  default, constrainFct, *constrainFctArgs, **constrainFctWArgs)
		self.objClassName = objClassName
		
	def check(self, val) :
		if val == self.default and self.default == None :
			return True
		return isRabaClass(val) and ((self.objClassName != None and val._rabaClass.__name__ == self.objClassName) or self.objClassName == None) and RabaField.check(self, val)

	def __repr__(self) :
		return '<field %s, class: %s , default: %s>' % (self.__class__.__name__, self.objClassName, self.default)

#lists
class PrimitiveList(RabaFieldList) :
	_raba_list = True
	_raba_type = RABA_FIELD_TYPE_IS_PRIMITIVE
	
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		RabaFieldList.__init__(self, Primitive, default, constrainFct, *constrainFctArgs, **constrainFctWArgs)

#class SerializedObjectList(RabaFieldList) :
#	_raba_type = RABA_FIELD_TYPE_IS_SERIALIZED_OBJECT
	
#	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
#		RabaFieldList.__init__(self, SerializedObject, default, constrainFct, *constrainFctArgs, **constrainFctWArgs)
		
class RabaObjectList(RabaFieldList) :
	_raba_type = RABA_FIELD_TYPE_IS_RABA_OBJECT
	
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		RabaFieldList.__init__(self, RabaObject, default, constrainFct, *constrainFctArgs, **constrainFctWArgs)

def isRabaClass(v) :
	return hasattr(v, '_rabaClass')
	
def isField(v) :
	return hasattr(v.__class__, '_raba_field') and v.__class__._raba_field
	
def isList(v) :
	return hasattr(v.__class__, '_raba_list') and v.__class__._raba_list

def typeIsPrimitive(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_PRIMITIVE

def typeIsRabaObject(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_RABA_OBJECT

#def typeIsSerializedObject(v) :
#	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_SERIALIZED_OBJECT
	
def isPythonPrimitive(v) :
	primTypes = [types.IntType, types.LongType, types.FloatType, types.StringType, types.UnicodeType, types.BufferType, types.NoneType]
	for t in primTypes :
		if isinstance(v, t) : 
			return True
	return False
