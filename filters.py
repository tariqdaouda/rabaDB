import re, types

from setup import RabaConnection, RabaConfiguration
from Raba import *
import fields as RabaFields

#####
# Add arthmetic operations {'chro.x2 - chro.x1 >' : 10}
#####

#Usage:
#########
#Equivalent queries
#########
#get all exons of a given transcript
#f = RabaQuery(namespace, Exon)
#
#f.addFilter(**{'transcript' : myTrans})
#f.addFilter({'transcript' : myTrans})
#f.addFilter(transcript = myTrans)
#f.addFilter("transcript = myTrans")
#exons = f.run()
#
#of a given chromosome
#query by number
#f.addFilter("transcript->gene->chromosome = 22")
#query by object
#f.addFilter(**{'transcript->gene->chromosome' : myChro})
#exons = f.run()
#
##########
# AND/OR
##########
#f.addFilter(A1, A2, A3)
#f.addFilter(B1, B2)
#
#Means: (A1 AND A2 AND A3) OR (B1 AND B2)

class RabaQuery :

	def __init__(self, rabaClass, namespace = None) :
		"see reset"
		self.reset(rabaClass, namespace)

	def reset(self, rabaClass, namespace = None) :
		"""rabaClass can either be raba class of a string of a raba class name. In the latter case you must provide the namespace argument.
		If it's a Raba Class the argument is ignored. If you fear cicular importants use strings"""

		if type(rabaClass) is types.StringType :
			self._raba_namespace = namespace
			self.con = RabaConnection(self._raba_namespace)
		else :
			self.rabaClass = rabaClass
			self._raba_namespace = self.rabaClass._raba_namespace

		self.con = RabaConnection(self._raba_namespace)
		self.filters = []
		self.tables = set()

		#self.fctPattern = re.compile("\s*([^\s]+)\s*\(\s*([^\s]+)\s*\)\s*([=><])\s*([^\s]+)\s*")
		self.fieldPattern = re.compile("\s*([^\s\(\)]+)\s*([=><]|([L|l][I|i][K|k][E|e]))\s*(.+)")
		self.operators = set(['LIKE', '=', '<', '>', 'IS'])
		self.artOperators = set(['+', '-', '*', '/', '<', '>', '%', '=', '>=', '<=', '<>', '!='])

	def _parseArtOperators(self, k) :
		#TODO
		pass

	def addFilter(self, *lstFilters, **dctFilters) :
		"add a new filter to the query"

		dstF = {}
		if len(lstFilters) > 0 :
			if type(lstFilters[0]) is types.DictType :
				dstF = lstFilters[0]
				lstFilters = lstFilters[1:]

		if len(dctFilters) > 0 :
			dstF = dict(dstF, **dctFilters)

		filts = {}
		for k, v in dstF.iteritems() :
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
				joink, joinv = self._parseJoint(sk[0], operator, vv)
				filts[joink] = joinv
			else :
				filts[kk] = vv

		for lt in lstFilters :
			for l in lt :
				match = self.fieldPattern.match(l)
				if match == None :
					raise ValueError("RabaQuery Error: Invalid filter '%s'" % l)

				field = match.group(1)
				operator = match.group(2)
				value = match.group(4)

				if field.find('->') > -1 :
					joink, joinv = self._parseJoint(field, operator, value)
					filts[joink] = joinv
				else :
					filts['%s.%s %s' %(self.rabaClass.__name__, field, operator)] = value

		self.filters.append(filts)

	def _parseJoint(self, strJoint, lastOperator, value) :
		def testAttribute(currClass, field) :
			attr = getattr(currClass, field)
			assert RabaFields.fieldIsRabaObjectt(attr)
			if attr.className == None :
				raise ValueError('Attribute %s has no mandatory RabaClass' % field)

			if attr.classNamespace != None and attr.classNamespace != self._raba_namespace :
				raise ValueError("Can't perform joints accros namespaces. My namespace is: '%s', %s->%s's is: '%s'" % (self._raba_namespace, currClass.__name__, attr.className, attr.classNamespace))
			return attr

		fields = strJoint.split('->')
		conditions = []

		currClass = self.rabaClass
		self.tables.add(currClass.__name__)
		for f in fields[:1] :
			attr = testAttribute(currClass, f)
			self.tables.add(attr.className)
			conditions.append('%s.%s = %s.json' %(currClass.__name__, f, attr.className))

			currClass = self.con.getClass(attr.className)

		lastField = fields[-1].split('.')

		attr = testAttribute(currClass, lastField[0])
		conditions.append('%s.%s = %s.json' %(currClass.__name__, lastField[0], attr.className))
		if len(lastField) == 2 :
			conditions.append('%s.%s' %(attr.className, lastField[-1]))
		elif len(lastField) == 1 :
			if RabaFields.fieldIsRabaObjectt(attr) :
				conditions.append('%s.json' %(attr.className))
			else :
				conditions.append('%s' %(attr.className))
		else :
			raise ValueError('Invalid query ending with %s' % field[-1])

		self.tables.add(attr.className)
		return '%s %s' % (' AND '.join(conditions), lastOperator), value

	def getSQLQuery(self) :
		"Returns the query without performing it"
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

		return (sql, sqlValues)

	def iterRun(self) :
		"""Runs the query and returns an iterator. This much more efficient for large sets of data but
		yyou get results one by one."""

		sql, sqlValues = self.getSQLQuery()
		cur = self.con.execute(sql, sqlValues)

		for v in cur :
			yield RabaPupa(self.rabaClass, v[0])

	def run(self) :
		"Runs the query and returns the entire result"
		sql, sqlValues = self.getSQLQuery()
		cur = self.con.execute(sql, sqlValues)

		res = []
		for v in cur :
			res.append(RabaPupa(self.rabaClass, v[0]))

		return res

if __name__ == '__main__' :
	#import unittest
	RabaConfiguration('test', './dbTest_filters.db')

	class A(Raba) :
		_raba_namespace = 'test'
		name = RabaFields.Primitive(default = 'A')
		b = RabaFields.RabaObject('B')
		_raba_uniques = ['name']

	class B(Raba) :
		_raba_namespace = 'test'
		name = RabaFields.Primitive(default = 'B')
		c = RabaFields.RabaObject('C')
		_raba_uniques = ['name']

	class C(Raba) :
		_raba_namespace = 'test'
		name = RabaFields.Primitive(default = 'C')
		a = RabaFields.RabaObject('A')
		_raba_uniques = ['name']

	a = A()
	b = B()
	c = C()

	a.b = b
	b.c = c
	c.a = a

	a.save()

	rq = RabaQuery(A)
	rq.addFilter({'b->c' : c})
	#rq.addFilter(['b->c.name = C'])
	for a in rq.run() :
		print a
