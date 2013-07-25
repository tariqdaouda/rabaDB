from setup import *
from Raba import *
from filters import *
import re

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

def comm() :
	gene = Gene('TPST2')
	chro = Chromosome('22')
	print chro.genes[-1].name
	print chro.genes
	chro.genes.append(gene)
	chro.save()

def comm2() :
	#pattern = re.compile("(.+)\((.+)\)\s+(.+)")
	pattern = re.compile("\s*([^\s]+)\s*\(\s*([^\s]+)\s*\)\s*([=><])\s*([^\s]+)\s*")
	match = pattern.match("count (genes )   >    4")
	fctName = match.group(1)
	field = match.group(2)
	operator = match.group(3)
	value = match.group(4)

	print fctName, '-', field, '-', operator, '-', value


	pattern = re.compile("\s*([^\s]+)\s*([=><]|([L|l][I|i][K|k][E|e]))\s*(.+)")
	#match = pattern.match("count (genes )   >    4")
	match = pattern.match('id   =    "4"')
	field = match.group(1)
	operator = match.group(2)
	value = match.group(4)

	print field, '-', operator, '-', value

#comm2()

f = RabaQuery(Chromosome)
f.addFilter(**{'id' : '= "22"'})
f.addFilter(['id = "22"', 'count(genes) = 4'])
f.addFilter('count(genes) = 4', id = '= "22"')
print f.run(True)
for chro in f.run() :
	print chro
	print chro.genes
	print chro
	print chro.genes[0].name
