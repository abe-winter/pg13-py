============
pg13
============

.. image:: https://travis-ci.org/abe-winter/pg13-py.svg?branch=master

**install** with `pip install pg13`

**docs** at http://pg13.readthedocs.org/en/latest/

pg13 is a SQL evaluator for python designed for testing. Normally when you want to test an application with database dependencies, you have three dangerous options:

1. **artisanal mocking**: standard mocking frameworks make you specify the output of every DB call
2. **local db**: have a running copy of the database
3. **everything but**: test everything but the DB interaction

pg13 takes a different approach:

* SQL is simulated in python
* every test can create and populate its own lightweight database
* tests are completely deterministic
* parallelization is safe (because parallel tests have no chance of touching the same data)
* performance: 200+ tests per second on my laptop

See the github readme for examples. https://github.com/abe-winter/pg13-py
