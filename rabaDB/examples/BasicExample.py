from rabaDB.rabaSetup import *
RabaConfiguration('test', './dbTest_BasicExample.db')
import rabaDB.Raba as R
from rabaDB.filters import *
from rabaDB.fields import *

class Human(R.Raba) :
	_raba_namespace = 'test'

	name = rf.Primitive()
	cars = rf.Relation('Car')
	
	def __init__(self) :
		pass

class Car(R.Raba) :
	_raba_namespace = 'test'

	number = rf.Primitive()
	def __init__(self) :
		pass

if __name__ == '__main__':
	georges = Human()
	georges.name = 'Georges'
	for i in range(10) :
		car = Car()
		car.number = i
		georges.cars.append(car)

	georges.save()

	sameGeorges = Human(name = 'Georges')
