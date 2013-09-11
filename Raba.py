import sqlite3 as sq
import os, types, cPickle, random, json
from setup import RabaConnection, RabaConfiguration
import fields as RabaFields

class _Raba_MetaClass(type) :
	
	def __new__(cls, name, bases, dct) :
		if name != 'Raba' :
			fields = []
			columns = {}
			columnsLower = set()
			
			i = 1
			for k, v in dct.items():
				if RabaFields.isField(v) :
					fields.append(k)
					columns[k] = i
					columnsLower.add(k.lower())
					i += 1
			
			con = RabaConnection(dct['_raba_namespace'])
			uniqueStr = ''
			if '_raba_uniques' in dct :
				for c in dct['_raba_uniques'] :
					if len(c) > 1 :
						uniqueStr += 'UNIQUE%s ON CONFLICT REPLACE, ' % str(c)
					else :
						uniqueStr += 'UNIQUE(%s) ON CONFLICT REPLACE, ' % str(c[0])
					
			uniqueStr = uniqueStr[:-2]
				
			if not con.tableExits(name) :
				idStr = 'raba_id INTEGER PRIMARY KEY AUTOINCREMENT'
				if len(fields) > 0 :
					if len(uniqueStr) > -1 :			
						con.createTable(name, '%s, %s' % (idStr, ', '.join(list(fields))))
					else :
						con.createTable(name, '%s, %s, %s' % (idStr, ', '.join(list(fields)), uniqueStr))
				else :
					con.createTable(name, '%s' % idStr)
				
				sqlCons =  'INSERT INTO raba_tables_constraints (table_name, constraints) VALUES (?, ?)'
				con.cursor().execute(sqlCons, (name, uniqueStr))
				con.commit()
			else :
				cur = con.cursor()
				sql = 'SELECT constraints FROM raba_tables_constraints WHERE table_name = ?'
				cur.execute(sql, (name,))
				res = cur.fetchone()
				
				if res[0] != uniqueStr :
					raise FutureWarning('Warning: The unique contraints have changed from:\n\t%s\n\nto:\n\t%s.\n-Unique constraints modification is not supported yet-\n' %(res[0], uniqueStr))
					
				cur.execute('PRAGMA table_info(%s)' % name)
				tableColumns = set()
				fieldsToKill = []
				#Destroy field that have mysteriously desapeared
				for c in cur :
					if c[1] != 'raba_id' and c[1].lower() not in columnsLower :
						#drop list table
						#tableName = self.connection.getRabaListTableName(self._rabaClass, c[1])
						#if tableName != None :
						#	self.connection.dropTable(tableName)
						#	self.connection.unregisterRabaList(self._rabaClass, c[1])
				
						fieldsToKill.append('%s = NULL' % c[1])
					tableColumns.add(c[1].lower())
	
				if len(fieldsToKill) > 0 :
					sql = 'UPDATE %s SET %s WHERE 1;' % (name , ', '.join(fieldsToKill))
					cur.execute(sql)
	
				for k in columns :
					if k.lower() not in tableColumns :
						cur.execute('ALTER TABLE %s ADD COLUMN %s' % (name, k))
				con.commit()
			
			def _class_getAttr(self, k) :
				try :
					return getattr(self, self._fieldsLowCase[k.lower()])
				except :
					raise AttributeError("Raba type '%s' has no attribute '%s'" % (self.__name__, k))
			
			cls.__getattr__ = _class_getAttr
		
			columns['raba_id'] = 0
			dct['raba_id'] = RabaFields.Primitive()
			dct['columns'] = columns
		
			clsObj = type.__new__(cls, name, bases, dct)
			RabaConfiguration(dct['_raba_namespace']).registerRabaClass(clsObj)
			
			return clsObj
			
		return type.__new__(cls, name, bases, dct)

class RabaPupa(object) :
	"""One of the founding principles of RabaDB is to separate the storage from the code. Fields are stored in the DB while the processing only depends
	on your python code. This approach ensures a higher degree of stability by preventing old objects from lurking inside the DB before popping out of nowhere several decades afterwards. 
	According to this apparoach, raba objects are not serialised but transformed into pupas before being stored. A pupa is a very light object that contains only a reference
	to the raba object class, and it's unique raba_id. Upon asking for one of the attributes of a pupa, it magically transforms into a full fledged raba object. This process is completly transparent to the user. Pupas also have the advantage of being light weight and also ensure that the only raba objects loaded are those explicitely accessed, thus potentialy saving a lot of memory.
	For a pupa self._rabaClass refers to the class of the object "inside" the pupa.
	"""
	
	def __init__(self, classObj, uniqueId) :
		self._rabaClass = classObj
		self.raba_id = uniqueId
		self.__doc__ = classObj.__doc__
		self.bypassMutationAttr = set(['_rabaClass', 'raba_id', '__class__', '__doc__'])
		
	def __getattribute__(self, name) :
		def getAttr(name) :
			return object.__getattribute__(self, name)
			
		def setAttr(name, value) :
			object.__setattr__(self, name, value)
		
		if name in getAttr('bypassMutationAttr'):
			return object.__getattribute__(self, name)
		
		setAttr('__class__', getAttr('_rabaClass'))
		uniqueId = getAttr('raba_id')
		
		purge = getAttr('__dict__').keys()
		for k in purge :
			delattr(self, k)
			
		Raba.__init__(self, raba_id = uniqueId)
		
		return object.__getattribute__(self, name)
	
	def __repr__(self) :
		return "<Raba pupa: %s, raba_id %s>" % (self._rabaClass.__name__, self.raba_id)

class Raba(object):
	"All raba object must inherit from this class. If the class has no attribute raba_id, an autoincrement field raba_id will be created"	
	__metaclass__ = _Raba_MetaClass
	
	def __init__(self, **fieldsSet) :
		
		if self.__class__ == Raba :
			raise TypeError('Raba class should never be instanciated, use inheritance')

		self._rabaClass = self.__class__
		
		self.connection = RabaConnection(self._rabaClass._raba_namespace)
		self.rabaConfiguration =  RabaConfiguration(self._rabaClass._raba_namespace)
		self.columns = self.__class__.columns
		
		#Initialisation
		self.raba_id = None
		
		definedFields = []
		definedValues = []
		for k, v in fieldsSet.items() :
			if k.lower() in self.columns : 
				object.__setattr__(self, k, v)
				definedFields.append(k)
				definedValues.append(v)
		
		if len(definedValues) > 0 :
			strWhere = ''
			for k in definedFields :
				strWhere = '%s = ? AND' % k
			
			strWhere = strWhere[:-4]
			sql = 'SELECT * FROM %s WHERE %s' % (self.__class__.__name__, strWhere)
			#print sql
			cur = self.connection.cursor()
			cur.execute(sql, definedValues)
			res = cur.fetchone()
			if cur.fetchone() != None :
				raise KeyError("More than one object fit the arguments you've prodided to the constructor")
			
			if res != None :
				for k, i in self.columns.items() :
					if k != 'raba_id' :
						elmt = getattr(self.__class__, k)
						if RabaFields.typeIsPrimitive(elmt) :
							try :
								object.__setattr__(self, k, cPickle.loads(str(res[i])))
							except :
								object.__setattr__(self, k, res[i])
						elif RabaFields.typeIsRabaObject(elmt) :
							if res[i] != None :
								print res[i]
								val = json.loads(res[i])
								objClass = self.rabaConfiguration.getClass(val["className"])
								object.__setattr__(self, k, RabaPupa(objClass, val["raba_id"]))
					#else :
					#	object.__setattr__(self, self.columns[i], res[i])
		
	def autoclean(self) :
		"""TODO: Copies the table into a new one removing all the collumns that have all their values to NULL
		and drop the tables that correspond to these tables"""
		pass
	
	def pupa(self) :
		"""returns a pupa version of self"""
		return RabaPupa(self.__class__, self.raba_id)
		
	def save(self) :
		fields = []
		values = []
		rabalists = []
		cur = self.connection.cursor()
		for k, valType in self.__class__.__dict__.items() :
			if RabaFields.isField(valType) and k != 'raba_id':
				val = getattr(self, k)
				if not RabaFields.isRabaList(valType) :
					if val == valType.default :
						values.append(val)
						fields.append(k)
					elif not valType.check(val) :
						raise ValueError("Unable to set '%s' to value '%s'. Constrain function violation" % (k, val))
					elif RabaFields.typeIsPrimitive(valType) :
						if RabaFields.isPythonPrimitive(val):
							values.append(val)
						else :
							values.append(buffer(cPickle.dumps(val)))
						fields.append(k)
					elif RabaFields.typeIsRabaObject(valType) :
						if valType != None :
							val.save()
							encodingDct = {'className' : val._rabaClass.__name__, 'raba_id' : val.raba_id}
							values.append(json.dumps(encodingDct))
							fields.append(k)
				else :
					if val != None and val.__class__ is RabaList :
						val._save()
					else :
						raise ValueError("Unable to set '%s' to value '%s' because it is not a valid RabaList" % (k, val))

		if len(values) > 0 :
			if self.raba_id == None :
				sql = 'INSERT INTO %s (%s) VALUES (%s)' % (self.__class__.__name__, ','.join(fields), ','.join(['?' for i in range(len(fields))]))
				cur.execute(sql, values)
				self.raba_id = cur.lastrowid
			else :
				values.append(self.raba_id)
				sql = 'UPDATE %s SET %s = ? WHERE raba_id = ?' % (self.__class__.__name__, ' = ?, '.join(fields))
				cur.execute(sql, values)
		else :
			raise ValueError('class %s has no fields to save' % self.__class__.__name__)
		
		self.connection.commit()

	def __setattr__(self, k, v) :
		if hasattr(self.__class__, k) and RabaFields.isField(getattr(self.__class__, k)) :
			if not RabaFields.isRabaList(getattr(self.__class__, k)) :
				classType = getattr(self.__class__, k)
				if not classType.check(v) :
					raise ValueError("Unable to set '%s' to value '%s'. Constrain function violation" % (k, v))
			else :
				if v.__class__ is not RabaList and v.__class__ is not RabaListPupa :
					raise ValueError("Unable to set '%s' to value '%s'. Value is not a valid RabaList" % (k, v))
				
		object.__setattr__(self, k, v)
	
	def __getattribute__(self, k) :
		elmt = object.__getattribute__(self, k)
		if RabaFields.isRabaList(elmt) :
			elmt = RabaListPupa(namespace = self.__class__._raba_namespace, relationName = k, anchorObj = self)
		elif RabaFields.isField(elmt) :
			elmt = elmt.default
		
		return elmt
		
	def __getitem__(self, k) :
		return self.__getattribute__(k)

	def __setitem__(self, k, v) :
		if v.__class__ is RabaList :
			v.anchObj = self
			v.relationName = k
		
		object.__setitem__(self, k, v)

	def __hash__(self) :
		return self.__class__.__name__+str(self.uniqueId)
	
	def __repr__(self) :
		return "<Raba obj: %s, raba_id %s>" % (self._rabaClass.__name__, self.raba_id)
	
class RabaListPupa(list) :
	_isRabaList = True
	
	def __init__(self, namespace, relationName, anchorObj) :
		self.bypassMutationAttr = set(['relationName', 'anchorObj', '__class__', '_morph', 'length'])
		self._raba_namespace = namespace
		self.relationName = relationName
		self.anchorObj = anchorObj
		
		self.length = RabaConnection(self._raba_namespace).getRabaListSize(self.anchorObj.__class__, self.relationName)
		
	def _morph(self) :
		list.__setattr__(self, '__class__', RabaList)
		
		relName = list.__getattribute__(self, 'relationName')
		anchObj = list.__getattribute__(self, 'anchorObj')
		namespace = list.__getattribute__(self, '_raba_namespace')
		
		purge = list.__getattribute__(self, '__dict__').keys()
		for k in purge :
			delattr(self, k)
		
		RabaList.__init__(self, namespace = namespace, relationName = relName, anchorObj = anchObj)
	
	def __getitem__(self, i) :
		self._morph()
		return self[i]
		
	def __getattribute__(self, name) :
		if name in list.__getattribute__(self, "bypassMutationAttr") :
			return list.__getattribute__(self, name)
		
		relName = list.__getattribute__(self, 'relationName')
		anchObj = list.__getattribute__(self, 'anchorObj')
		list.__getattribute__(self, "_morph")()
		
		anchObj.__setattr__(relName, self)
		
		return list.__getattribute__(self, name)

	def __repr__(self) :
		return "<RLPupa relationName: %s, anchorObj: %s >" % (self.relationName, self.anchorObj)

	def __len__(self) :
		return self.length
		
class RabaList(list) :
	"""A RabaList is a list that can only contain Raba objects of the same class or (Pupas of the same class). They represent one to many relations and are stored in separate
	tables that contain only one single line"""
	
	_isRabaList = True
	
	def _checkElmt(self, v) :
		if not RabaFields.isRabaObject(v) or self._raba_namespace == None or (self._raba_namespace != None and v._raba_namespace == self._raba_namespace) :
			return True
		
		return False
		
	def _checkRabaList(self, v) :
		vv = list(v)
		for e in vv :
			if not self._checkElmt(e) :
				return (False, e)
		return (True, None)
	
	def _dieInvalidRaba(self, v) :
		raise TypeError('Only Raba objects of the same namespace can be stored in the same Raba List. List namespace is: %s, object: %s' % self._raba_namespace, v._raba_namespace)
	
	def __init__(self, *argv, **argk) :
		list.__init__(self, *argv)
		check = self._checkRabaList(self)
		if not check[0]:
			self._dieInvalidRaba(check[1])
		
		try :
			self._setNamespaceConAndConf(argk['namespace'])
			self.anchorObj = argk['anchorObj']
			self.relationName = argk['relationName']
		except (KeyError) :
			self._raba_namespace = None
			self.anchorObj = None
			self.relationName = None
		
		if self.relationName != None  :
			tableName = self._makeTableName(self.relationName, self.anchorObj._rabaClass)
			cur = self.connection.cursor()
			if not self.connection.tableExits(tableName) :
				self.connection.createTable(tableName, 'anchor_id, value_or_id, type, PRIMARY KEY(anchor_id, value_or_id)')
				self.connection.registerRabalistTable(self.anchorObj._rabaClass, self.relationName, tableName)

			cur = self.connection.cursor()
			cur.execute('SELECT * FROM %s WHERE anchor_id = ?' % tableName, (self.anchorObj.raba_id, ))
			for aidi in cur :
				elmt = aidi[1]
				typ = aidi[2]
				if typ ==  RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE :
					self.append(elmt)
				else :
					self.append(RabaPupa(RabaConfiguration(self._raba_namespace).getClass(typ), elmt))
	
	def _setNamespaceConAndConf(self, namespace) :
		self._raba_namespace = namespace
		self.connection = RabaConnection(self._raba_namespace)
		self.configuration = RabaConfiguration(self._raba_namespace)
		
	def pupatizeElements(self) :
		"""Transform all raba object into pupas"""
		for i in range(len(self)) :
			self[i] = self[i].pupa()

	def _erase(self, relationName , anchorObj) :
		tableName = self._makeTableName(relationName, anchorObj._rabaClass)
		cur = self.connection.cursor()
		cur.execute('UPDATE %s SET anchor_id = NULL, value_or_id = NULL, type = NULL WHERE anchor_id = ?' % tableName, (anchorObj.raba_id,))
	
	def _save(self) :
		"""saves the RabaList into it's own table. This a private function that should be called directly
		Before saving the entire list corresponding to the anchorObj is wiped out before being rewritten. The
		alternative would be to keep the sync between the list and the table in real time (remove in both).
		If the current solution proves to be to slow, i'll consider the alternative"""
		
		if len(self) > 0 :
			if self.relationName == None or self.anchorObj == None :
				raise ValueError('%s has not been attached to any object, impossible to save it' % s)
			self._erase(self.relationName , self.anchorObj)
			
			values = []
			for e in self :
				if RabaFields.isRabaObject(e) :
					e.save()
					values.append((self.anchorObj.raba_id, e.raba_id, self.anchorObj.__class__.__name__))
				else :
					values.append((self.anchorObj.raba_id, e.raba_id, RabaFields.RABA_FIELD_TYPE_IS_PRIMITIVE))
			
			tableName = self._makeTableName(self.relationName, self.anchorObj._rabaClass)
			self.connection.cursor().executemany('INSERT INTO %s (anchor_id, value_or_id, type) VALUES (?, ?, ?)' % tableName, values)
			self.connection.commit()
			
			self.connection.updateRabaListSize(self.relationName, len(self))
	
	def _makeTableName(self, relationName, anchorClass) :
		return 'RabaList_%s_for_%s' % (relationName, anchorClass.__name__)
	
	def extend(self, v) :
		check = self._checkRabaList(v)
		if not check[0]:
			self._dieInvalidRaba(check[1])
		list.extend(self, v)
		if self._raba_namespace == None :
			self._setNamespaceConAndConf(self[0]._raba_namespace)

	def append(self, v) :
		if not self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.append(self, v)
		if self._raba_namespace == None :
			self._setNamespaceConAndConf(self[0]._raba_namespace)
		
	def insert(self, k, v) :
		if not self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.insert(self, k, v)
		if self._raba_namespace == None :
			self._setNamespaceConAndConf(self[0]._raba_namespace)
	
	def __setitem__(self, k, v) :
		if self._checkElmt(v) :
			self._dieInvalidRaba(v)
		list.__setitem__(self, k, v)

	def __repr__(self) :
		return '<RL'+list.__repr__(self)+'>'
