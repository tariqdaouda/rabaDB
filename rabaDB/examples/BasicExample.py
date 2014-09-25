from rabaDB.setup import *
RabaConfiguration('test', './dbTest_BasicExample.db')
import rabaDB.Raba as R
from rabaDB.filters import *
from rabaDB.fields import *

class transcript(R.Raba) :
	_raba_namespace = 'test'
	name = Primitive(default = '')
	gene = RabaObject('gene')
	def __init__(self, **fieldsSet) :
		pass

class Gene(R.Raba) :
	_raba_namespace = 'test'
	name = Primitive(default = '')
	chromosome = RabaObject('Chromosome')
	def __init__(self, **fieldsSet) :
		pass

class Chromosome(R.Raba) :
	_raba_namespace = 'test'
	id = Primitive(default = '')
	genes = Relation()
	genes_2 = RList()
	def __init__(self, **fieldsSet) :
		pass


print '\nCreate a gene'
gene = Gene()
gene.set(name = 'TPST2')
print '\nCreate or load the chromosome 22'

try :
	chro = Chromosome(id = '22')
except :
	chro = Chromosome()
	chro.set(id = '22')

print '\nAdd the genes TPST2 the chromosome'
chro.genes.append(gene)
print '\nName of last gene in list'
print chro.genes[-1].name
print '\nthe complete list'
print chro.genes
print '\nsave the chromosome'
chro.save()

f = RabaQuery('test', transcript)
f.addFilter(**{'gene->chromosome.id' : "22"})
print f.run()

