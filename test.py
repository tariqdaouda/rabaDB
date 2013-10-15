class RabaPupaSingleton(type):
	_instances = {}
	def __call__(cls, *args, **kwargs):
		print 'call', cls, args, kwargs
		key = '%s%d' % (str(args[0]), args[1])
		print key
		if cls not in cls._instances:
			cls._instances[cls] = super(RabaPupaSingleton, cls).__call__(*args, **kwargs)
		
		v = cls._instances[cls]
		print 'v', v
		return cls._instances[cls]
	
	def __new__(cls, name, bases, dct) :
		print 'new'
		return type.__new__(cls, name, bases, dct)

class A(object) :
	__metaclass__ = RabaPupaSingleton
	def __init__(self, a, b) :
		print "aa"
		self.a = a

rs = A(5, 4)
print rs.a, rs
rs = A(8, 4)
print rs.a
