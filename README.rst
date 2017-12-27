.. image:: http://depsy.org/api/package/pypi/rabaDB/badge.svg
   :alt: depsy
   :target: http://depsy.org/package/python/rabaDB
 
Lightweight Object Persistency for python.
=========================================================

Installation
-------------

.. code:: python
	
	pip install rabaDB #for the latest stable version

That's it, no need for anything else.

What's rabaDB?
--------------

rabaDB is a lightweight, borderline NoSQL, uncomplicated, ORM on top of sqlite3.

Basicly it means:

* *Uncomplicated syntax*
* *No SQL knowledge needed*: supports queries by example
* *You can still use SQL* if you want
* *Lazy and Optimized*: objects are only fully loaded if you need them to be
* *No dependencies*: super easy installation 
* *Somewhat Schemaless*: you can modify field definitions whenever you want and it will automatically adapt to the new schema

Can it be used for "big projects"?
----------------------------------

rabaDB is the backend behind pyGeno_, a python package for genomics and proteomics where it is typically used to store
whole genomes annonations, along with huge sets of polymorphisms: millions of entries. and it's performing really well.

.. _pyGeno: https://github.com/tariqdaouda/pyGeno

99% of what you need to know in one Example
--------------------------------------------

.. code:: python
	
	#The initialisation
	from rabaDB.rabaSetup import *
	RabaConfiguration('test_namespace', './dbTest_BasicExample.db')
	import rabaDB.Raba as R
	import rabaDB.fields as rf
	
	class Human(R.Raba) :
		_raba_namespace = 'test_namespace'
		
		#Everything that is not a raba object is primitive
		name = rf.Primitive()
		age = rf.Primitive()
		city = rf.Primitive()
		
		#Only Cars can fit into this relation
		cars = rf.Relation('Car')
		
		#best friend can only be a human
		bestFriend = rf.RabaObject('Human')
		
		def __init__(self) :
			pass
	
	class Car(R.Raba) :
		_raba_namespace = 'test_namespace'
	
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
	
		#saving georges also saves all his cars to the db
		georges.save()



What's a namespace?
-------------------

You can think of rabaDB's namespace as independent databases. Each namespace has a name and a file where all the data
will be saved. Here's how you initialise rabaDb:

.. code:: python
	
	#The initialisation
	from rabaDB.rabaSetup import *
	RabaConfiguration('test', './dbTest_BasicExample.db')

Once you've done that, the configuration is a singleton attached to the namespace. If the filename does not exist
it will be created for you.

You can access it everywhere in you script by simply doing

.. code:: python
	
	myConf = RabaConfiguration('test')

There's also a connection object associated to the namespace

.. code:: python
	
	myConn = RabaConnnection('test')

To know what you can do with that, have a look at the debugging part.

Field types
-----------

RabaDB has only **four** variable types:

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

Schemaless?
-----------

rabaDB allows you to change the schemas of your classes on the fly. That means that you can add and remove fields
from your class definitions at any moment during the developement and rabaDB will take care of composing with the
SQL backend. However keep in mind that whenever you remove a field, all the information relative to that field
are lost forever.

You can even erase whole class definitions from you code, and rabaDB will automatically update the database.

Indexation
-----------
No problem:

.. code:: python
	
	Human.ensureIndex('name')
	#even on several fields
	Human.ensureIndex(('name', 'age', 'city'))
	
	#To drop an index
	Human.dropIndex('name')

Brutal Querying 
---------------

You can do things like:

.. code:: python

	georges = Human(name = 'Georges')

And rabaDB will try to find a match for you.

Querying by example
-------------------
Querying by example is done by creating filters, all the conditions inside the same filter are merged by **And**
and filters between them are merged by **Or**.

.. code::

	f = RabaQuery(SomeClass)
	
	f.addFilter(A1, A2, A3)
	f.addFilter(B1, B2)
	
	Means: (A1 AND A2 AND A3) OR (B1 AND B2)

There are several syntaxes that you can use.

.. code:: python

	from rabaDB.filters import *
	
	f = RabaQuery(Human)
	#Or
	f = RabaQuery('Human')
	
	f.addFilter(name = "Fela", age = "22")
	#Or the fancier
	f.addFilter({"age >=" : 22, "name like": "fel%"})
	#Or
	f.addFilter(['age = "22"', 'name = Fela'])

And then here's how you get your results:

.. code:: python

	for r in f.run() :
		print r
	
You can add an SQL statement at the end

.. code:: python
	
	for r in f.run(sqlTail = "ORDER By age") :
		print r
	

Querying SQL style
------------------

You can also write your own SQL *WHERE* conditions

.. code:: python

	from rabaDB.filters import *
	
	f = RabaQuery(Human)
	
	for r in f.runWhere("age = ?, name = ?" , (22, "fela")) :
		print r


Getting raw SQL 
----------------

By default all querying functions return raba Object, but you can always ask for the raw **SQL** tuple:

.. code:: python
	
	f.run(raw = True)
	f.runWere(("age = ?, name = ?" , (22, "fela"), raw = True)

Supported operators for queries
--------------------------------

The supported operators are: 'LIKE', '=', '<', '>', '=', '>=', '<=', '<>', '!=', 'IS'

Yes, but I just want to loop through the results  
------------------------------------------------

There are also iterative versions. They have the same interface but they are faster and less memory consuming
	
	* f.iterRun
	* f.iterRunWhere

And counts?
----------

Here's how you do counts

.. code:: python

	from rabaDB.filters import *
	
	f = RabaQuery(Human)
	f.addFilter(age = "22")
	print f.count()

Registry
---------

rabaDB keeps an internal registry to ensure a strong object consistency. If you do:

.. code:: python
	
	georges = Human(name = 'Georges')
	sameGeorges = Human(name = 'Georges')

You get two times the same object, every modification you do to georges is also applied to sameGeorges,
because georges **is** sameGeorges. This rules applies to any form of queries.

However keep in mind that the registery will also prevent the garbage collector from erasing raba objects, and
that can lead to "memory leak"-like situations. The way that is by telling raba that you
no longer need some objects to be registered:

.. code:: python

	form rabaDB.Raba import *
	
	_unregisterRabaObjectInstance(georges)


Debugging
---------

RabaDB has debugging tools that you can access through the namespace's connection.
 
 .. code:: python
 
 	import rabaDB.rabaSetup
 	conn = rabaDB.rabaSetup.RabaConnection("mynamespace")
	
	#printing the SQL queries
	conn.enableQueryPrint(True)
	#the part you want to debug
	conn.enableQueryPrint(False)
	#debug: print each SQL querie and asks the permition to continue
	conn.enableDebug(True)
	#the part you want to debug
	conn.enableDebug(False)
	
	#record all the queries performed
	conn.enableStats(True, logQueries = True)
	#the part you want to debug
	conn.enableStats(False)
	#a pretty print
	conn.printStats()
	
	#when you're done
	conn.eraseStats()
	
Transactions
------------

You can group several queries into one single transaction

 .. code:: python
 
 	conn.beginTransaction()
 	#a lot of object saving
 	conn.endTransaction()

Inheritence
-----------

rabaDB fully supports inheritence. Children classes automatically inherit the fields of their parents.
rabaDB also supports abstract classes, that is to say classes that are never meant to be instanciated and that only
serve as templates for other classes. Abstract classes have no effect on the database

Here's how you declare an abstract class:

.. code:: python
	
	class pyGenoRabaObject(Raba) :

		_raba_namespace = "pyGeno"
		_raba_abstract = True # abstractness
		
		name = rf.Primitive()
		
