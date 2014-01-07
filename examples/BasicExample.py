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

class Gene(R.Raba) :
	_raba_namespace = 'test'
	name = Primitive(default = '')
	chromosome = RabaObject('Chromosome')
	def __init__(self, **fieldsSet) :

class Chromosome(R.Raba) :
	_raba_namespace = 'test'
	id = Primitive(default = '')
	genes = Relation()
	genes_2 = RList()
	def __init__(self, **fieldsSet) :



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
'''
#---------
#Different types of equivalent queries
#---------

f = RabaQuery('test', Chromosome)
f.addFilter(**{'id' : "22"})
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
'''
