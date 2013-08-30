from setup import *
from Raba import *
import re, types

class RabaQuery :
	
	def __init__(self, namespace, rabaType) :
		self.rabaType = rabaType
		self.filters = []
		self._raba_namespace = namespace
		self.con = RabaConnection(self._raba_namespace)
		
		self.fctPattern = re.compile("\s*([^\s]+)\s*\(\s*([^\s]+)\s*\)\s*([=><])\s*([^\s]+)\s*")
		self.fieldPattern = re.compile("\s*([^\s\(\)]+)\s*([=><]|([L|l][I|i][K|k][E|e]))\s*(.+)")
		
		self.supportedFunctions = set(('count', ))
		
	def addFilter(self, *lstFilters, **dctFilters) :
		"add a new filter to the query"
		strFilters = []#list(lstFilters)
		for v in lstFilters :
			if isinstance(v, types.ListType) or isinstance(v, types.TupleType) :
				for v2 in v :
					strFilters.append(v2)
			else :
				strFilters.append(v)
				
		for k, v in dctFilters.items() :
			strFilters.append('%s %s' % (k, v))
		
		for i in range(len(strFilters)) :
			if self._parseField(strFilters[i]) :
				pass
			else :
				resFct = self._parseFct(strFilters[i])
				if resFct != None :
					 strFilters[i] = resFct
				else :
					raise ValueError("RabaQuery Error: Invalid filter '%s'" % f)

		self.filters.append(strFilters)
	
	def _parseFct(self, wholeStr) :

		match = self.fctPattern.match(wholeStr)
		if match == None :
			return None

		fctName = match.group(1)
		field = match.group(2)
		operator = match.group(3)
		value = match.group(4)

		if not hasattr(self.rabaType, field) :
			raise KeyError("RabaQuery Error: type '%s' has no field %s" % (self.rabaType.__name__, field))
		if not isRabaType(getattr(self.rabaType, field)) :
			raise TypeError("RabaQuery Error: the parameter of '%s' must a be RabaType" % fctName.upper())
		
		if fctName.lower() == 'count' :
			return '%s %s %s' % (field, operator, value)
		else :
			raise ValueError("RabaQuery Error: Unknown function %s" % fctName.upper())
			
	def _parseField(self, wholeStr)	:
		
		match = self.fieldPattern.match(wholeStr)
		if match == None :
			return False

		field = match.group(1)
		#operator = match.group(2)
		#value = match.group(4)
		
		if not hasattr(self.rabaType, field) :
			raise KeyError("RabaQuery Error: type '%s' has no field %s" % (self.rabaType.__name__, field))

		return True
		
	def run(self, returnSQL = False) :
		"Runs the query and returns the result"
		sqlFilters = []
		for f in self.filters :
			sqlFilters.append('(%s)' % ' AND '.join(f))
			
		sqlFilters = ' OR '.join(sqlFilters)
		sql = 'SELECT id from %s WHERE %s' % (self.rabaType.__name__, sqlFilters)
		#print sql
		cur = self.con.cursor()
		cur.execute(sql)
		
		res = []
		for v in cur :
			res.append(RabaPupa(self.rabaType, v[0]))
		
		if returnSQL :
			return (res, sql)
		else :
			return res
