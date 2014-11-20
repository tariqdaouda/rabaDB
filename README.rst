Save, Search, Modify your objects easily. You're welcome.
=========================================================

Installation:
-------------

.. code:: python
	
	pip install rabaDB #for the latest stable version

That's it, no need for anything else.

What's rabaDB:
-------------

rabaDB is a lightweight uncomplicated schemaless ORM on top of sqlite3.

Basicly it means:

* Uncomplicated syntax
* Lazy and Optimized: objects are only fully loaded if you need them to be
* Supports Queries by examples and SQL
* Super easy installation, no dependencies

Can it be used for "big projects":
---------------------------------

rabaDB is the backend behind pyGeno_, a python package for genomics and protemics where it is typically used to store
whole genomes annonations, along with huge sets of polymorphisms, and it's performing really well.

.. _pyGeno: https://github.com/tariqdaouda/pyGeno


Example:
-------

RabaDB has only four variable types:

*Primitive:
	- Numbers
	- Strings
	- Serialized objects
*RabaObject
	- An object whose class derives from Raba.Raba
*Relation:
	- A list of only a certain type of RabaObject 
*RList:
	- A list of anything

.. code:: python
	
	#The initialisation
	from rabaDB.rabaSetup import *
	RabaConfiguration('test', './dbTest_BasicExample.db')
	import rabaDB.Raba as R
	import rabaDB.fields as rf
	
	class Human(R.Raba) :
		_raba_namespace = 'test'
		
		#Everything that is not a raba object is primitive
		name = rf.Primitive()
		
		#Only Cars can fit into this relation
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
