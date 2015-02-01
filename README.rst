============
pg13
============

.. image:: https://travis-ci.org/abe-winter/pg13-py.svg?branch=master

**install** with `pip install pg13`

**docs** at http://pg13.readthedocs.org/en/latest/

pg13 is a SQL evaluator for python designed for testing. Normally when you want to test an application with database dependencies, you have three equally bad options:

1. **artisanal mocking**: standard mocking frameworks make you specify the output of every DB call

 * bad because it's extra work, maintenance nightmare, and you're feeding the test the right answer

2. **local db**: have a running copy of the database

 * bad because your tests are less portable, slower, and may have inter-test data dependencies

3. **everything but**: test everything but the DB interaction

 * bad because you're not testing a big part of your app

pg13 takes a different approach:

* SQL is simulated in python
* every test can create and populate its own lightweight database
* tests are completely deterministic
* parallelization is safe (because parallel tests have no chance of touching the same data)
* performance: about 100 tests per second on my laptop

See the github readme for examples. https://github.com/abe-winter/pg13-py
