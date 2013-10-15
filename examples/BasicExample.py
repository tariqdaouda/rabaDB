from rabaDB.setup import *
RabaConfiguration('test', './dbTest_BasicExample.db')
from rabaDB.Raba import *
from rabaDB.filters import *
from rabaDB.fields import *

class Gene(Raba) :
	_raba_namespace = 'test'
	name = PrimitiveField(default = '')
	def __init__(self, **fieldsSet) :
		Raba.__init__(self, **fieldsSet)

class Chromosome(Raba) :
	_raba_namespace = 'test'
	id = PrimitiveField(default = '')
	genes = RabaListField()
	def __init__(self, **fieldsSet) :
		Raba.__init__(self, **fieldsSet)
		
#Create a gene
gene = Gene(name = 'TPST2')
#Create a chromosome
chro = Chromosome(id = '22')
#Add the gene to the chromosome
chro.genes.append(gene)
chro.genes.append(Gene(name = 'TPST2_2'))
#print chro.genes[-1].name
#print chro.genes
#save the chromosome
#chro.save()

#---------
#Different types of equivalent queries
#---------

f = RabaQuery('test', Chromosome)
f.addFilter(**{'id' : '= "22"'})
f.addFilter(['id = "22"', 'count(genes) = 4'])
f.addFilter('count(genes) = 4', id = '= "22"')
for chro in f.run() :
	print "The chromosome is here but not fully loaded"
	print chro
	print "The genes are still not fully loaded"
	print chro.genes
	print "Now that we've acceced one of it's attributes, the chromosome fully loaded"
	print chro
	print "The name of the first gene"
	print chro.genes[0].name
	print "Only the first gene of the list is fully loaded"
	print chro.genes
