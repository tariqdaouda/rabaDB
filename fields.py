import types

class Primitive(object) :
	_raba_field = True
	_raba_list = False
	_raba_primitive = True
	
	def __init__(self, default = None, constrainFct = None, *constrainFctArgs, **constrainFctWArgs) :
		self.default = default
		self.constrainFct = constrainFct
		self.constrainFctArgs = constrainFctArgs
		self.constrainFctWArgs = constrainFctWArgs
		
	def check(self, val) :
		if self.constrainFct == None :
			return True
		
		return self.constrainFct(val, *self.constrainFctArgs, **self.constrainFctWArgs)

class PrimitiveList(object) :
	_raba_field = True
	_raba_list = True
	_raba_primitive = True
	
class Object(object) :
	_raba_field = True
	_raba_list = False
	_raba_object = True
	
class ObjectList(object) :
	_raba_field = True
	_raba_list = True
	_raba_object = True
	
def isField(v) :
	return hasattr(v.__class__, '_raba_field') and v.__class__._raba_field
	
def isList(v) :
	return hasattr(v.__class__, '_raba_list') and v.__class__._raba_list

def isPrimitive(v) :
	return hasattr(v.__class__, '_raba_primitive') and v.__class__._raba_primitive

def isObject(v) :
	return hasattr(v.__class__, '_raba_object') and v.__class__._raba_object
	
def isPythonPrimitive(v) :
	primTypes = [types.IntType, types.LongType, types.FloatType, types.StringType, types.UnicodeType, types.BufferType, types.NoneType]
	for t in primTypes :
		if isinstance(v, t) : 
			return True
	return False
