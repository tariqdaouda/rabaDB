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
	genes_2 = RabaListField()
	def __init__(self, **fieldsSet) :
		Raba.__init__(self, **fieldsSet)

"""
print 'Create a gene'
gene = Gene(name = 'TPST2')
print 'Create or load the chromosome 22'
chro = Chromosome(id = '22')
print 'Add the genes TPST2 and TPST3 to the chromosome'
chro.genes.append(gene)
chro.genes.append(Gene(name = 'TPST3'))
print 'Name of last gene in list'
print chro.genes[-1].name
print 'the complete list'
print chro.genes
print 'save the chromosome'
chro.save()
"""
#---------
#Different types of equivalent queries
#---------

f = RabaQuery('test', Chromosome)
f.addFilter(**{'id' : '= "22"'})
f.addFilter(['id = "22"', 'count(genes) = 4'])
f.addFilter('count(genes) = 4', id = '= "22"')
for chro2 in f.run() :
	#print chro is chro2
	print "The chromosome is here but not fully loaded"
	print '\t', chro2
	print "The genes are still not fully loaded"
	print '\t', chro2.genes
	print "Now that we've acceced one of it's attributes, the chromosome is fully loaded"
	print '\t', chro2
	print "The name of the first gene"
	#print '\t', chro2.genes
	print '\t', chro2.genes[0].name
	#print '\t', chro2.genes[1].name
	print "Only the first gene of the list is fully loaded"
	print '\t', chro2.genes

f = RabaQuery('test', Chromosome)
f.addFilter(**{'id' : '= "22"'})
f.addFilter(['id = "22"', 'count(genes) = 4'])
f.addFilter('count(genes) = 4', id = '= "22"')

for chro2 in f.run() :
	print '\t', chro2.genes
