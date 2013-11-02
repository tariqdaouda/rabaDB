import re, types

from setup import *
from Raba import *
import fields as RabaFields

#Usage:
#
#get all exons of a given transcript
#f = RabaQuery(namespace, Exon)
#f.addFilter(**{'transcript' : transcript})
#transcript.exons = f.run()

#TODO JOINTS:
#
#get all exons of a given transcript
#f = RabaQuery(namespace, Exon)
#f.addFilter(**{'self.transcript.gene' : gene})
#transcript.exons = f.run()


class RabaQuery :
	
	def __init__(self, namespace, rabaType) :
		self.rabaType = rabaType
		self.filters = {}
		self._raba_namespace = namespace
		self.con = RabaConnection(self._raba_namespace)
		
		self.fctPattern = re.compile("\s*([^\s]+)\s*\(\s*([^\s]+)\s*\)\s*([=><])\s*([^\s]+)\s*")
		self.fieldPattern = re.compile("\s*([^\s\(\)]+)\s*([=><]|([L|l][I|i][K|k][E|e]))\s*(.+)")
		
		self.supportedFunctions = set(('count', ))
	
	def addFilter(self, *lstFilters, **dctFilters) :
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
		self.filters[len(self.filters) +1] = filters
	
	def _parseInput(self, wholeStr) :
		res = self._parseField(wholeStr)
		if res == None :
			res = self._parseFct(wholeStr)
			if res == None :
				raise ValueError("RabaQuery Error: Invalid filter '%s'" % wholeStr)
		return res
	
	def _parseField(self, wholeStr)	:
		match = self.fieldPattern.match(wholeStr)
		if match == None :
			return None

		field = match.group(1)
		operator = match.group(2)
		value = match.group(4)
		if not hasattr(self.rabaType, field) :
			raise KeyError("RabaQuery Error: type '%s' has no field %s" % (self.rabaType.__name__, field))
		
		return '%s %s' %(field, operator), value
		
	def _parseFct(self, wholeStr) :
		
		match = self.fctPattern.match(wholeStr)
		if match == None :
			return False

		fctName = match.group(1)
		field = match.group(2)
		operator = match.group(3)
		value = match.group(4)

		if not hasattr(self.rabaType.__class__, field) or not RabaFields.isField(getattr(self.rabaType.__class__, field)) :
			raise KeyError("RabaQuery Error: type '%s' has no field %s" % (self.rabaType.__name__, field))
		if not RabaFields.typeIsRabaList(getattr(self.rabaType.__class__, field)) :
			raise TypeError("RabaQuery Error: the parameter of '%s' must be a RabaList" % fctName.upper())
		
		if fctName.lower() == 'count' :
			return '%s %s' %(field, operator), value
		else :
			raise ValueError("RabaQuery Error: Unknown function %s" % fctName.upper())

	def run(self, returnSQL = False) :
		"Runs the query and returns the result"
		sqlFilters = []
		sqlValues = []
		for f in self.filters.values() :
			sqlFilters.append('(%s ?)' % ' ? AND '.join(f.keys()))
			sqlValues.extend(f.values())
			
		sqlFilters = ' OR '.join(sqlFilters)
		sql = 'SELECT raba_id from %s WHERE %s' % (self.rabaType.__name__, sqlFilters)
		#print sql, sqlValues
		cur = self.con.cursor()
		cur.execute(sql, sqlValues)
		
		res = []
		for v in cur :
			res.append(RabaPupa(self.rabaType, v[0]))
		
		if returnSQL :
			return (res, sql)
		else :
			return res
