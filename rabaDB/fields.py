from . import Raba

RABA_FIELD_TYPE_IS_UNDEFINED = -1
RABA_FIELD_TYPE_IS_PRIMITIVE = 0
RABA_FIELD_TYPE_IS_RABA_OBJECT = 1
RABA_FIELD_TYPE_IS_RABA_LIST = 2

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

class RList(RabaField) :
	_raba_field = True
	_raba_list = True
	_raba_type = RABA_FIELD_TYPE_IS_RABA_LIST

	def __init__(self, ElmtConstrainFct = None, **ElmtConstrainFctWArgs) :
		RabaField.__init__(self, default = None, constrainFct = ElmtConstrainFct, **ElmtConstrainFctWArgs)
		del(self.default)# = None

class Relation(RList) :
	def __init__(self, className = None, ElmtConstrainFct = None, **ElmtConstrainFctWArgs) :
		self.className = className
		RList.__init__(self, ElmtConstrainFct, **ElmtConstrainFctWArgs)

	def check(self, val) :
		return Raba.isRabaObject(val) and ((self.className != None and val._rabaClass.__name__ == self.className) or self.className == None) and RList.check(self, val)

class Primitive(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_PRIMITIVE

	def __init__(self, default = None, constrainFct = None, **constrainFctWArgs) :
		RabaField.__init__(self,  default, constrainFct, **constrainFctWArgs)

	def check(self, val) :
		return RabaField.check(self, val)

	def __repr__(self) :
		return '<field %s, default: %s>' % (self.__class__.__name__, self.default)

class RabaObject(RabaField) :
	_raba_type = RABA_FIELD_TYPE_IS_RABA_OBJECT

	def __init__(self, className = None, classNamespace = None, default = None, constrainFct = None, **constrainFctWArgs) :
		"""rabaClass can either be raba class of a string of a raba class name. In the latter case you must provide the namespace argument.
		If it's a Raba Class the argument is ignored. If you fear cicular importants use strings"""

		if default != None and not isRabaObject(default) :
			raise ValueError("Default value is not a valid Raba Object")

		RabaField.__init__(self,  default, constrainFct, **constrainFctWArgs)
		if type(className) is not str :
			assert isRabaClass(className)
			self.className = RabaConnection(className._raba_namespace).getClass(className.__name__)
			self.classNamespace = className._raba_namespace
		else :
			self.className = className
			self.classNamespace = classNamespace

	def check(self, val) :
		if val == self.default and self.default == None :
			return True
		retVal =  Raba.isRabaObject(val) and ((self.className != None and val._rabaClass.__name__ == self.className) or self.className == None) and RabaField.check(self, val)

		if self.classNamespace == None :
			return retVal
		else :
			return retVal and val._raba_namespace == self.classNamespace

	def __repr__(self) :
		return '<field %s, class: %s , default: %s>' % (self.__class__.__name__, self.className, self.default)


def isField(v) :
	return hasattr(v.__class__, '_raba_field') and v.__class__._raba_field

def isPrimitiveField(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_PRIMITIVE

def isRabaObjectField(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_RABA_OBJECT

def isRabaListField(v) :
	return hasattr(v.__class__, '_raba_type') and v.__class__._raba_type == RABA_FIELD_TYPE_IS_RABA_LIST

