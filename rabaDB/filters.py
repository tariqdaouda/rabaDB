import re
import warnings

from . import rabaSetup as stp
from .Raba import *
from . import fields as RabaFields

#####
# TODO
#------
# - Add COUNT (if field is rabalist return valeur de la case, else select count())
# - Add arthmetic operations {'chro.x2 - chro.x1 >' : 10}
#####

#Usage:
#########
#Equivalent queries
#########
#get all exons of a given transcript
#f = RabaQuery(Exon, namespace)
#
#f.addFilter(**{'transcript' : myTrans})
#f.addFilter({'transcript' : myTrans})
#f.addFilter(transcript = myTrans)
#f.addFilter("transcript = myTrans")
#exons = f.run()
#
#of a given chromosome
#query by number
#f.addFilter("transcript.gene.chromosome = 22")
#query by object
#f.addFilter(**{'transcript.gene.chromosome' : myChro})
#exons = f.run()
#
# #All exons :
# f = RabaQuery(Exon)
# f.run()
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
		"""rabaClass can either be a raba class or a string of a raba class name. In the latter case you must provide the namespace argument.
		If it's a Raba Class the argument is ignored. If you fear cicular imports use strings"""

		if type(rabaClass) is str :
			self._raba_namespace = namespace
			self.con = stp.RabaConnection(self._raba_namespace)
			self.rabaClass = self.con.getClass(rabaClass)
		else :
			self.rabaClass = rabaClass
			self._raba_namespace = self.rabaClass._raba_namespace

		self.con = stp.RabaConnection(self._raba_namespace)
		self.filters = []
		self.tables = set()

		#self.fctPattern = re.compile("\s*([^\s]+)\s*\(\s*([^\s]+)\s*\)\s*([=><])\s*([^\s]+)\s*")
		self.fieldPattern = re.compile("\s*([^\s\(\)]+)\s*([=><]|([L|l][I|i][K|k][E|e]))\s*(.+)")
		self.operators = set(['LIKE', '=', '<', '>', '=', '>=', '<=', '<>', '!=', 'IS'])
		#self.artOperators = set(['+', '-', '*', '/', '%'])

	def _parseAritOperators(self, k) :
		#TODO
		pass

	def addFilter(self, *lstFilters, **dctFilters) :
		"add a new filter to the query"
		
		dstF = {}
		if len(lstFilters) > 0 :
			if type(lstFilters[0]) is dict :
				dstF = lstFilters[0]
				lstFilters = lstFilters[1:]

		if len(dctFilters) > 0 :
			dstF = dict(dstF, **dctFilters)

		filts = {}
		for k, v in dstF.items() :
			sk = k.split(' ')
			if len(sk) == 2 :
				operator = sk[-1].strip().upper()
				if operator not in self.operators :
					raise ValueError('Unrecognized operator "%s"' % operator)
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

			if sk[0].find('.') > -1 :
				kk = self._parseJoint(sk[0], operator)
			
			filts[kk] = vv
				
		for lt in lstFilters :
			for l in lt :
				match = self.fieldPattern.match(l)
				if match == None :
					raise ValueError("RabaQuery Error: Invalid filter '%s'" % l)

				field = match.group(1)
				operator = match.group(2)
				value = match.group(4)

				if field.find('.') > -1 :
					joink = self._parseJoint(field, operator, value)
					filts[joink] = value
				else :
					filts['%s.%s %s' %(self.rabaClass.__name__, field, operator)] = value

		self.filters.append(filts)

	def _parseJoint(self, strJoint, lastOperator) :
		fields = strJoint.split('.')
		conditions = []

		currClass = self.rabaClass
		self.tables.add(currClass.__name__)
		for f in fields[:1] :
			attr = getattr(currClass, f)
			if not RabaFields.isRabaObjectField(attr) :
				raise ValueError("Attribute %s is not for a Raba Object, can't process join" % f)

			self.tables.add(attr.className)
			conditions.append('%s.%s = %s.json' %(currClass.__name__, f, attr.className))

			currClass = self.con.getClass(attr.className)

		lastAttr = getattr(currClass, fields[-1])
		if RabaFields.isRabaObjectField(lastAttr) :
			conditions.append('%s.json' %(lastAttr.className))
		else :
			conditions.append('%s.%s' %(attr.className, fields[-1]))

		self.tables.add(attr.className)
		return '%s %s' % (' AND '.join(conditions), lastOperator)

	def getSQLQuery(self, count = False) :
		"Returns the query without performing it. If count, the query returned will be a SELECT COUNT() instead of a SELECT"
		sqlFilters = []
		sqlValues = []
		# print self.filters
		for f in self.filters :
			filt = []
			for k, vv in f.items() :
				if type(vv) is list or type(vv) is tuple :
					sqlValues.extend(vv)
					kk = 'OR %s ? '%k * len(vv)
					kk = "(%s)" % kk[3:]
				else :
					kk = k
				sqlValues.append(vv)
				filt.append(kk)	
			
			sqlFilters.append('(%s ?)' % ' ? AND '.join(filt))
		
		if len(sqlValues) > stp.SQLITE_LIMIT_VARIABLE_NUMBER :
			raise ValueError("""The limit number of parameters imposed by sqlite is %s.
You will have to break your query into several smaller one. Sorry about that. (actual number of parameters is: %s)""" % (stp.SQLITE_LIMIT_VARIABLE_NUMBER, len(sqlValues)))
		
		sqlFilters =' OR '.join(sqlFilters)
		
		if len(self.tables) < 2 :
			tablesStr = self.rabaClass.__name__
		else :
			tablesStr =  ', '.join(self.tables)
		
		if len(sqlFilters) == 0 :
			sqlFilters = '1'
		if count :
			sql = 'SELECT COUNT(*) FROM %s WHERE %s' % (tablesStr, sqlFilters)
		else :
			sql = 'SELECT %s.raba_id FROM %s WHERE %s' % (self.rabaClass.__name__, tablesStr, sqlFilters)
		
		return (sql, sqlValues)

	def iterRun(self, sqlTail = '', raw = False) :
		"""Compile filters and run the query and returns a generator. This much more efficient for large data sets but
		you get the results one element at a time. One thing to keep in mind is that this function keeps the cursor open, that means that the sqlite databae is locked (no updates/inserts etc...) until all
		the elements have been fetched. For batch updates to the database, preload the results into a list using get, then do you updates.
		You can use sqlTail to add things such as order by
		If raw, returns the raw tuple data (not wrapped into a raba object)"""

		warnings.warn("At some point in the future 'iterRun' will stop existing and will get replaced by 'run'. If you have functions still using the iter method, you should start changing them now. These changes are in conformity with python3 as opposed to the now-deprecated python2.", DeprecationWarning)
		
		sql, sqlValues = self.getSQLQuery()
		cur = self.con.execute('%s %s'% (sql, sqlTail), sqlValues)
		for v in cur :
			if not raw :
				yield RabaPupa(self.rabaClass, v[0])
			else :
				yield v
			
	def run(self, sqlTail = '', raw = False, gen=False):
		"""Compile filters and run the query and returns the entire result. You can use sqlTail to add things such as order by. If raw, returns the raw tuple data (not wrapped into a raba object)"""
		
		warnings.warn("At some point in the future 'run' will be returning a generator by default (gen=True). If you want to use the output in a list format, you can just wrap your call with list(). These changes are in conformity with python3 as opposed to the now-deprecated python2.", DeprecationWarning)		
		
		if gen:
			return self.iterRun(sqlTail, raw)
		else:
			sql, sqlValues = self.getSQLQuery()
			cur = self.con.execute('%s %s'% (sql, sqlTail), sqlValues)
			
			res = []
			for v in cur:
				if not raw :
					res.append(RabaPupa(self.rabaClass, v[0]))
				else:
					return v
				return res
	
	def count(self, sqlTail = '') :
		"Compile filters and counts the number of results. You can use sqlTail to add things such as order by"
		sql, sqlValues = self.getSQLQuery(count = True)
		return int(self.con.execute('%s %s'% (sql, sqlTail), sqlValues).fetchone()[0])
		
	def runWhere(self, whereAndTail, params = (), raw = False, gen=False):
		"""You get to write your own where + tail clauses. If raw, returns the raw tuple data (not wrapped into a raba object).If raw, returns the raw tuple data (not wrapped into a raba object)"""
		
		warnings.warn("At some point in the future 'runWhere' will be returning a generator by default (generator=True). If you want to use the output in a list format, you can just wrap your call with list(). These changes are in conformity with python3 as opposed to the now-deprecated python2.", DeprecationWarning)
		
		if gen:
			return self.iterRunWhere(whereAndTail, params, raw)
		else:
			sql = "SELECT %s.raba_id FROM %s WHERE %s" % (self.rabaClass.__name__, self.rabaClass.__name__, whereAndTail)
			cur = self.con.execute(sql, params)
			res = []
			for v in cur :
				if not raw :
					res.append(RabaPupa(self.rabaClass, v[0]))
				else:
					return v
			return res

	def iterRunWhere(self, whereAndTail, params = (), raw = False) :
		"""You get to write your own where + tail clauses. If raw, returns the raw tuple data (not wrapped into a raba object).If raw, returns the raw tuple data (not wrapped into a raba object).
		For more info see iterGet()
		"""
		
		warnings.warn("At some point in the future 'iterRunWhere' will stop existing and will get replaced by 'runWhere'. If you have functions still using the iter method, you should start changing them now. These changes are in conformity with python3 as opposed to the now-deprecated python2.", DeprecationWarning)
		
		sql = "SELECT %s.raba_id FROM %s WHERE %s" % (self.rabaClass.__name__, self.rabaClass.__name__, whereAndTail)
		cur = self.con.execute(sql, params)
		res = []
		for v in cur :
			if not raw :
				yield RabaPupa(self.rabaClass, v[0])
			else :
				yield v

if __name__ == '__main__' :
	#import unittest
	stp.RabaConfiguration('test', './dbTest_filters.db')

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
		print(a)
