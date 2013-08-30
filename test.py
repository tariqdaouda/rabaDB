class RabaNameSpaceSingleton(type):
	_instances = {}
	def __call__(cls, *args, **kwargs):
		key = '%s%s' % (cls, args[0])
		if cls not in cls._instances:
			cls._instances[key] = super(RabaNameSpaceSingleton, cls).__call__(*args, **kwargs)
		
		return cls._instances[key]

class A :
	__metaclass__ = RabaNameSpaceSingleton
	def __init__(self, a) :
		self.a = a

a = A('a')
b = A('b')
c = A('a')

print a.a
print b.a
print c.a
