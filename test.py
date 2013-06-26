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

	def op(self) :
		print 'op'
	

