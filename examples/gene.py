from rabaDB.setup import *
RabaConfiguration('test', './dbTest.db')
from rabaDB.Raba import *
from rabaDB.filters import *

class Gene(Raba) :
	_raba_namespace = 'test'
	
	rabaId = Autoincrement
	chromo = RabaObj()
	genome = RabaObj()
	geneId = ''
	name = ''
	
	_uniques = (('chromo', 'genome', 'geneId'))
	
	def __init__(self, name, chromo, uniqueId = None) :
		Raba.__init__(self, uniqueId)
		self.name = name
		self.chromosomes = RabaList()
