from setup import *
RabaConnection('./test.db')
from Raba import *

RabaConnection('raba-db1.db')
#RabaConnection().dropTable('Gene')
#RabaConnection().dropTable('vache')
class Gene(Raba) :
	id = Autoincrement
	name = "TPST2"
	def __init__(self, name, uniqueId = None) :
		self.name = name
		Raba.__init__(self, uniqueId)
	
class Vache(Raba) :
	id = None
	genes = RabaType(Gene)
	def __init__(self, uniqueId = None) :
		Raba.__init__(self, uniqueId)

v = Vache("vache1")
print 'befor append', v.genes
v.genes.append(Gene('sss'))
print 'after apped', v.genes
v.save()
print 'after apped', v.genes
