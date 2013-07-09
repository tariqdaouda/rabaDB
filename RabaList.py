import sqlite3 as sq
import os, types
import setup as conf
from Raba import *

class RabaListPupa(object) :
	
	def __init__(self, relationName, anchorObj, elmtsClassObj) :
		self.relationName = relationName
		self.anchorObj = anchorObj
		self.elmtsClassObj = elmtsClassObj
	
	def __getattribute__(self,name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
	
		setAttr('__class__', getAttr('classObj'))
		RabaList.__init__(self, getAttr('relationName'), getAttr('anchorObj'), getAttr('elmtsClassObj'))
		
		return object.__getattribute__(self, name)

class RabaList(list) :
	"""A RabaList is a list that can only contain Raba objects of the same class or (Pupas of the same class). They represent one to many relations and are stored in separate
	tables that contain only one single line"""
	
	def _checkElmt(self, v) :
		if not _isRabaClass(v) :
			return False
			
		if len(self) > 0 and v._rabaClass != self[0]._rabaClass and (v._rabaClass != RabaPupa or v.elmtsClassObj != self[0]._rabaClass) :
			return False
		
		return True
		
	def _checkRabaList(self, v) :
		vv = list(v)
		for e in vv :
			if not self._checkElmt(e) :
				return (False, e)
		return (True, None)
	
	def _dieInvalidRaba(self, v) :
		raise TypeError('Only Raba objects of the same class can be stored in RabaLists. Elmt: %s is not a valid RabaObject' % v)
			
	def __init__(self, *argv, **argk) :
		list.__init__(self, *argv, **argk)
		check = self._checkRabaList(self)
		if not check[0]:
			self._dieInvalidRaba(check[1])
			
		try :
			tableName = RabaList._makeTableName(argk['relationName'], argk['elmtsClassObj'], argk['anchorObj']._rabaClass)
			cur = RABA_CONNECTION.cursor()
			cur.execute('SELECT * FROM %s' % tableName)
			for aidi in cur :
				self.append(RabaPupa(argk['elmtsClassObj'], aidi[0]))
				
		except KeyError:
			pass
			
	def extend(self, v) :
		check = self._checkRabaList(v)
		if not check[0]:
			self._dieInvalidRaba(check[1])
		list.extend(self, v)			
	
	def append(self, v) :
		if not self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.append(self, v)

	def insert(self, k, v) :
		if not self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.insert(self, k, v)
	
	def pupatizeElements(self) :
		"""Transform all raba object into pupas"""
		for i in range(len(self)) :
			self[i] = self[i].pupa()

	def _save(self, relationName , anchorObj) :
		"""saves the RabaList into it's own table. This a private function that should be called directly"""
		if len(self) > 0 :
			tableName = RabaList._makeTableName(relationName, self[0]._rabaClass , anchorObj_rabaClass)
		
			cur = conf.RABA_CONNECTION.cursor()
			cur.execute('DROP TABLE IF EXITS %s' % tableName)
			cur.execute('CREATE TABLE %s(id)' % tableName)
			values = []
			for e in self :
				values.append((e.id, ))
			cur.executemany('INSERT INTO %s (id) VALUES (?)' % tableName, values)
			RABA_CONNECTION.commit()

	def _makeTableName(relationName, elmtsRabaClass, anchorObjRabaClass) :
		"#ex: RabaList_non-synsnps(snps)_of_gene_ENSG"
		return 'RabaList_%s(%s)_of_%s_%s' % (relationName, elmtsRabaClass, anchorObjRabaClass.__class__.__name__, anchorObj.id)
		
	def __setitem__(self, k, v) :
		if self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)

if __name__ == '__main__' :
	class Gene(Raba) :
		id = None
		name = "TPST2"
		def __init__(self, name, uniqueId = None) :
			self.name = name
			Raba.__init__(self, uniqueId)
		
	class Vache(Raba) :
		id = None
		genes = Rabalist()
		def __init__(self, uniqueId = None) :
			Raba.__init__(self, uniqueId)

v = Vache()
v.genes.append(Gene('sss'))
	
