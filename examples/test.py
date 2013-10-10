from rabaDB.setup import *
RabaConfiguration('test', './dbTest.db')
from rabaDB.Raba import *
from rabaDB.fields import *
from rabaDB.filters import *

def constF(v, k) :
	#print k
	return True

class Gene(Raba) :
	_raba_namespace = 'test'
	name = PrimitiveField(default = 'TPST2')
	def __init__(self, **fieldsSet) :
		Raba.__init__(self, **fieldsSet)
	
class Chromosome(Raba) :
	_raba_namespace = 'test'
	number = PrimitiveField(default = '22', constrainFct = None)
	genes = RabaListField()
	snps = RabaListField(ElmtConstrainFct = constF, k = 5)
	genome = RabaObjectField('Gene', default = None)
	_raba_uniques = [('genome', 'number'), ('genes', )]
	def __init__(self, **fieldsSet) :
		Raba.__init__(self, **fieldsSet)

c = Chromosome(raba_id = 1)
#print c.snps, len(c.snps), c.raba_id

print '---->', c.snps, c.snps[3:], type(c.snps[3:])
print '---->', c.snps, c.snps[3:][::-1], type(c.snps[3:]), c.snps[3]
#c.snps = RabaList(range(10))
#c.snps.append(5)
#print c.snps, type(c.snps[0]), type(c.snps[3:]), c.snps[3:]
#c.snps.append(Gene())
#print '---->', c.snps, c.snps[3:], type(c.snps[3:])
#c.snps = RabaList()
#print '---->', c.snps
#print "in chromo", c.genes
#c.genes.append(Gene())
#print '+++', c.genes, c.genes[0].name
#c.save()
