from rabaDB.setup import *
RabaConfiguration('test', './dbTest.db')
from rabaDB.Raba import *
from rabaDB.filters import *

import gene

class Chromosome(Raba) :
	_raba_namespace = 'test'
	id = '22'
	genes = RabaType(gene.Gene)
	def __init__(self, uniqueId = None) :
		Raba.__init__(self, uniqueId)

#print '=====', Raba.__subclasses__()

c = Chromosome()
