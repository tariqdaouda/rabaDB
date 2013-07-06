import sqlite3 as sq
import os, types
import setup as conf

#TODO
#TYPES doivent d'abord dependre des sous class de RABA
#si non present create table, si table mais non present drop table
#
#updateType dois s'occuper juste de la table relation, les table des types doient etre gere par Raba
#
#La metaclass n'a pas acces au fields self pour creer la table du type. Ajouter les champs suplementaire au save du Raba
#
#Rabalistes


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
		if not _isRabaType(v) :
			return False
			
		if len(self) > 0 and v.__class__ != self[0].__class__ and (v.__class__ != RabaPupa or v.elmtsClassObj != self[0].__class__) :
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
			self.elmtsClassObj = argk['elmtsClassObj']
			tableName = self._makeTableName(argk['relationName'], argk['anchorObj'])
			cur = RABA_CONNECTION.cursor()
			cur.execute('SELECT * FROM %s' % tableName)
			for aidi in cur :
				self.append(RabaPupa(self.elmtsClassObj, aidi[0]))
				
		except KeyError:
			self.elmtsClassObj = None
			
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
			tableName = self._makeTableName(relationName , anchorObj)
		
			cur = conf.RABA_CONNECTION.cursor()
			cur.execute('DROP TABLE IF EXITS %s' % tableName)
			cur.execute('CREATE TABLE %s(id)' % tableName)
			values = []
			for e in self :
				values.append((e.id, ))
			cur.executemany('INSET INTO %s (id) VALUES (?)' % tableName, values)
			RABA_CONNECTION.commit()

	def _makeTableName(self, relationName , anchorObj) :
		"#ex: RabaList_non-synsnps(snps)_of_gene_ENSG"
		return 'RabaList_%s(%s)_of_%s_%s' % (relationName, self.elmtsClass, anchorObj.__class__.__name__, anchorObj.id)
		
	def __setitem__(self, k, v) :
		if self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)
