import sqlite3 as sq
import os

RABA_CONNECTION = None
TYPES = None

#DISCLAIMER: Black Magic
#This performs the magical trick of hooking the objectification to the SQL databse that works behind the scenes.

def tableExits(name) :
	global RABA_CONNECTION
	sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" % name
	return RABA_CONNECTION.cursor().execute(sql).fetchone() != None
	
def init(rabaDBStorage = 'rabaDB') :
	if not os.path.exists(rabaDBStorage) :
		print "raba storage path %s not found, creating it..." % rabaDBStorage
		os.makedirs(rabaDBStorage)
	
def connect(dbName = 'rabaDB-0.db') :
	global RABA_CONNECTION
	RABA_CONNECTION = sq.connect(dbName)
	
	if not tableExits('Relations') :
		print "table Relations not found, creating it..."
		sql = 'CREATE TABLE Relations (id INTEGER PRIMARY KEY AUTOINCREMENT);'
		RABA_CONNECTION.cursor().execute(sql)
		RABA_CONNECTION.commit()

	return RABA_CONNECTION
	
#------

class Relations :
	__metaclass__ = type
	def __init__(self) :
		self.types = set()
		col = RABA_CONNECTION.cursor().execute('PRAGMA table_info(Relations)')
		for c in col :
			self.types.add(c[0])
			
	def hasType(self, name) :
		return name in self.types
	
	def addType(self, name) :
		if not self.hasType(name) :
			sql = 'ALTER TABLE Relations ADD %s;' % name
			self.types = RABA_CONNECTION.cursor().execute(sql)
			RABA_CONNECTION.commit()
			self.types.add(name)
	
	def removeType(self, name) :
		if not self.hasType(name) :
			sql = 'ALTER TABLE Relations drop %s;' % name
			self.types = RABA_CONNECTION.cursor().execute(sql)
			RABA_CONNECTION.commit()
			self.types.remove(name)	

def setTypes() :
	global TYPES
	TYPES = set()
	col = RABA_CONNECTION.cursor().execute('PRAGMA table_info(Relations)')
	for c in col :
		TYPES.add(c[1])
	return TYPES
	
def hasType(name) :
	global TYPES
	return name in TYPES
	
def addType(name) :
	if not hasType(name) :
		sql = 'ALTER TABLE Relations ADD %s;' % name
		TYPES = RABA_CONNECTION.cursor().execute(sql)
		RABA_CONNECTION.commit()
		TYPES.add(name)

def removeType(name) :
	if not self.hasType(name) :
		sql = 'ALTER TABLE Relations drop %s;' % name
		TYPES = RABA_CONNECTION.cursor().execute(sql)
		RABA_CONNECTION.commit()
		TYPES.remove(name)	

class RabaType_MetaClass(type) :
	def __init__(cls, name, bases, dct) :
		if not hasType(name) :
			addType(name)
		super(RabaType_MetaClass, cls).__init__(name, bases, dct)
		
class RabaType:
	__metaclass__ = type #RabaType_MetaClass
	__rabatype__ = True
	
	def __init__(self, uniqueId = None) :
		self.load(uniqueId)

	def load(self, uniqueId = None) :
		self.fields = {}	
		col = RABA_CONNECTION.cursor().execute('PRAGMA table_info(%s)' % self.__class__.__name__)
		for c in col :
			self.fields[c[0]] = None
		
		if uniqueId != None :
			sql = ('SELECT * FROM %s WHERE 1' % self.__class__.__name__)
			cur = RABA_CONNECTION.cursor()
			cur.execute(sql)
			#for cur.
			
	def __getitem__(k) :
		return self.fields[k]

	def __setitem(k, v) :
		self.fields[k] = v


init()
RABA_CONNECTION = connect()
setTypes()
print TYPES
#addType('CDS')
#===============
"""
r = request()
r.select('genes')
f = r.getFilter()
f.add('symbol', 'HLA...') r.filter(symbol = 'HLA...'), r.exclude(symbol='HLA')
f.add('symbol', 'IG...')
<=> HLA or IG
res = r.get()

f = r.getFilter()
f.add('symbol', 'HLA...')
f.count('exons', '> 3')
f2 = r.getFilter()
f2.add('symbol', 'IG...')
f.count('exons', '> 5')
res r.get()
<=> (HLA and count exons > 3) or (IG and count exons > 5)
"""
