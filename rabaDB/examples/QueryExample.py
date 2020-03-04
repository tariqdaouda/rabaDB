from setup import *
from Raba import *
from filters import *

class Gene(Raba) :
	id = Autoincrement
	name = ''
	def __init__(self, name, uniqueId = None) :
		Raba.__init__(self, uniqueId)
		self.name = name

class Chromosome(Raba) :
	id = '22'
	genes = RabaType(Gene)
	def __init__(self, uniqueId = None) :
		Raba.__init__(self, uniqueId)


#---------
#Different types of equivalent queries
#---------

f = RabaQuery(Chromosome)
f.addFilter(**{'id' : '= "22"'})
f.addFilter(['id = "22"', 'count(genes) = 4'])
f.addFilter('count(genes) = 4', id = '= "22"')
for chro in f.run() :
	print("The chromosome is here but not fully loaded")
	print(chro)
	print("The genes are still not fully loaded")
	print(chro.genes)
	print("Now that we've accessed one of it's attributes, the chromosome is fully loaded")
	print(chro)
	print("The name of the first gene")
	print(chro.genes[0].name)
	print("Only the first gene of the list is fully loaded")
	print(chro.genes)
