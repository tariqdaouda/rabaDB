rabaDB:
=======

rabaDB is a lightweight uncomplicated schemaless ORM on top of sqlite3.
For more about how to use it you can check the examples folder.

.. code:: python
	
	from rabaDB.rabaSetup import *
	RabaConfiguration('test', './dbTest_BasicExample.db')
	import rabaDB.Raba as R
	import rabaDB.fields as rf
	
	class Human(R.Raba) :
		_raba_namespace = 'transPep'
	
		name = rf.Primitive()
		cars = rf.Relation('Car')
		
		def __init__(self) :
			pass
	
	class Car(R.Raba) :
		_raba_namespace = 'transPep'
	
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
