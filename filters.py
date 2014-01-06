import re, types

from setup import *
from Raba import *
import fields as RabaFields

#Usage:
#
#get all exons of a given transcript
#f = RabaQuery(namespace, Exon)
#f.addFilter(**{'transcript' : myTrans})
#f.addFilter(transcript = myTrans)
#f.addFilter("transcript = myTrans")
#exons = f.run()

class RabaQuery :

	def __init__(self, rabaClass) :
		self.reset(rabaClass)
	
	def reset(self, rabaClass) :
		assert isRabaClass(rabaClass)
		
		self.rabaClass = rabaClass
		self.filters = []
		self.tables = []
		
		self._raba_namespace = self.rabaClass._raba_namespace
		self.con = RabaConnection(self._raba_namespace)
		
		#self.fctPattern = re.compile("\s*([^\s]+)\s*\(\s*([^\s]+)\s*\)\s*([=><])\s*([^\s]+)\s*")
		self.fieldPattern = re.compile("\s*([^\s\(\)]+)\s*([=><]|([L|l][I|i][K|k][E|e]))\s*(.+)")
		self.operators = set(['LIKE', '=', '<', '>', 'IS'])
		
	def addFilter_bck(self, *lstFilters, **dctFilters) :
		"add a new filter to the query"
		filters = {}
		for v in lstFilters :
			if isinstance(v, types.ListType) or isinstance(v, types.TupleType) :
				for v2 in v :
					res = self._parseInput(v2)
					filters[res[0]] = res[1]
			else :
				self._parseInput(v)
				filters[res[0]] = res[1]
		
		operators = set(['LIKE', '=', '<', '>', 'IS'])
		for k, v in dctFilters.items() :
			if k[-1].upper() not in operators :
				kk = '%s =' % k
			else :
				kk = k
			
			try :
				res = self._parseInput('%s %s' %(kk, v.getJsonEncoding()))
				vv = v.getJsonEncoding()
			except :
				res = self._parseInput('%s %s' %(kk, v))
				vv = v
			
			filters[res[0]] = vv
		self.filters.append(filters)

	def addFilter(self, *lstFilters, **dctFilters) :
		"add a new filter to the query"
		
		for k, v in dctFilters.iteritems() :
			sk = k.split(' ')
			if len(sk) == 2 :
				operator = sk[-1].strip()
				if operator not in self.operators :
					raise ValueError('Unrecognized operator %s' % operator)
				kk = '%s.%s'% (self.rabaClass.__name__, k)
			elif len(sk) == 1 :
				operator = "="
				kk = '%s.%s ='% (self.rabaClass.__name__, k)
			else :
				raise ValueError('Invalid field %s' % k)
			
			if isRabaObject(v) :
				vv = v.getJsonEncoding()
			else :
				vv = v
				
			if sk[0].find('->') > -1 :
				self._parseJoint(sk[0], operator, vv)
			else :
				self.filters.append({kk : vv})

		for lt in lstFilters :
			for l in lt : 
				match = self.fieldPattern.match(l)
				if match == None :
					raise ValueError("RabaQuery Error: Invalid filter '%s'" % l)
				
				field = match.group(1)
				operator = match.group(2)
				value = match.group(4)
				
				if field.find('->') > -1 :
					self._parseJoint(field, operator, value)
				else :
					self.filters.append({'%s.%s %s' %(self.rabaClass.__name__, field, operator) : value})
			
	def _parseJoint(self, strJoint, lastOperator, value) :
		def testAttribute(currClass, field) :
			attr = getattr(currClass, field)
			assert RabaFields.typeIsRabaObject(attr)
			if attr.objClassName == None :
				raise ValueError('Attribute %s has no mandatory RabaClass' % field)

			if attr.objClassNamespace != None and attr.objClassNamespace != self._raba_namespace :
				raise ValueError("Can't perform joints accros namespaces. My namespace is: '%s', %s->%s's is: '%s'" % (self._raba_namespace, currClass.__name__, attr.objClassName, attr.objClassNamespace))
			return attr
		
		fields = strJoint.split('->')
		conditions = []
		
		currClass = self.rabaClass
		self.tables.append(currClass.__name__)
		for f in fields[:1] :
			attr = testAttribute(currClass, f)
			self.tables.append(attr.objClassName)
			conditions.append('%s.%s = %s.json' %(currClass.__name__, f, attr.objClassName))
			
			currClass = self.con.getClass(attr.objClassName)
		
		lastField = fields[-1].split('.')
		
		attr = testAttribute(currClass, lastField[0])
		conditions.append('%s.%s = %s.json' %(currClass.__name__, lastField[0], attr.objClassName))
		if len(lastField) == 2 :
			conditions.append('%s.%s' %(attr.objClassName, lastField[-1]))
		elif len(lastField) == 1 :
			conditions.append('%s' %(attr.objClassName))
		else :
			raise ValueError('Invalid query ending with %s' % field[-1])
			
		self.tables.append(attr.objClassName)
		self.filters.append({ '%s %s' % (' AND '.join(conditions), lastOperator) : value})
		
	def run(self, returnSQL = False) :
		"Runs the query and returns the result"
		sqlFilters = []
		sqlValues = []
		#print self.filters
		for f in self.filters :
			sqlFilters.append('(%s ?)' % ' ? AND '.join(f.keys()))
			sqlValues.extend(f.values())
		
		sqlFilters =' OR '.join(sqlFilters)
		
		if len(self.tables) < 2 :
			tablesStr = self.rabaClass.__name__
		else :
			tablesStr =  ', '.join(self.tables)
		
		sql = 'SELECT %s.raba_id from %s WHERE %s' % (self.rabaClass.__name__, tablesStr, sqlFilters)
		#print sql, sqlValues
		cur = self.con.cursor()
		cur.execute(sql, sqlValues)
		
		res = []
		for v in cur :
			res.append(RabaPupa(self.rabaClass, v[0]))
		
		if returnSQL :
			return (res, sql)
		else :
			return res

if __name__ == '__main__' :
	import unittest
	RabaConfiguration('test', './dbTest_filters.db')
	
	class A(Raba) :
		_raba_namespace = 'test'
		name = RabaFields.PrimitiveField(default = 'A')
		b = RabaFields.RabaObjectField('B')
		_raba_uniques = ['name']
		
		def __init__(self, **fieldsSet) :
			Raba.__init__(self, **fieldsSet)
	
	class B(Raba) :
		_raba_namespace = 'test'
		name = RabaFields.PrimitiveField(default = 'B')
		c = RabaFields.RabaObjectField('C')
		_raba_uniques = ['name']
		
		def __init__(self, **fieldsSet) :
			Raba.__init__(self, **fieldsSet)
	
	class C(Raba) :
		_raba_namespace = 'test'
		name = RabaFields.PrimitiveField(default = 'C')
		a = RabaFields.RabaObjectField('A')
		_raba_uniques = ['name']
		
		def __init__(self, **fieldsSet) :
			Raba.__init__(self, **fieldsSet)

	a = A()
	b = B()
	c = C()
	
	a.b = b
	b.c = c
	c.a = a
	
	a.save()
	
	
	rq = RabaQuery(A)
	rq.addFilter(**{'b->c' : c})
	#rq.addFilter(['b->c.name = C'])
	print rq.run()
