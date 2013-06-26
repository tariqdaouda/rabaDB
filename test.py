class RabaPrimitive(object) :
	def __new__(cls, *args, **kwargs) :
		class RabaPrimitive(args[0].__class__) :
			__op__ = True
		
		return RabaPrimitive(*args, **kwargs)


a = RabaPrimitive('sss')
print a, hasattr(a, '__op__')

class B :
	def __init__(self) :
		pass

class A :
	a = 3
	def __init__(self) :
		self.s = 's'
		self.b = B()
		
a = A()
print a.__class__.__dict__
print type(a.__dict__['b']).__name__
print dir(a)
