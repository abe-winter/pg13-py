# pg13 [![Build Status](https://travis-ci.org/abe-winter/pg13-py.svg?branch=master)](https://travis-ci.org/abe-winter/pg13-py)

**install** with `pip install pg13[psyco]`

**docs** at http://pg13.readthedocs.org/en/latest/

pg13 is a SQL ORM for python designed with first-class support for mocking & test. Normally when you want to test an application with database dependencies, you have three dangerous options:

1. **artisanal mocking**: standard mocking frameworks make you specify the output of every DB call
 * bad because it's extra work, tough to maintain, and you're feeding the test the right answer
2. **local db**: have a running copy of the database
 * but your tests gain an external dependency (the DB)
 * this encourages hidden inter-test data dependencies
 * this can interfere with local integration environments
3. **everything but**: test everything but the DB interaction
 * bad because you're not testing a big part of your app

pg13 takes a different approach:
* SQL is simulated in python
* every test can create and populate its own lightweight database
* tests are deterministic (at least with respect to database reuse)
* parallelization across tests is safe (at least with respect to data)
* performance: ~200 tests per second on my laptop

Drop me a line if you're using this. `@gmail: awinter.public` (hint: turn it around)

## examples

Note: everything below is happening in-python and in-memory. Each instance (table dictionary) is completely isolated so your tests can run in parallel or whatever, you don't need a live DB on your system. Interacting with a live database looks exactly the same as below except for creating the pool and the pool.tables lines.
```python
from pg13 import pg,pgmock
class Model(pg.Row):
  FIELDS = [('userid','int'),('id2','int'),('content','text')]
  PKEY = 'userid,id2'
  TABLE = 'model'
  @pg.dirty('content')
  def content_len(self):
    "this will get cached until the 'text' field is changed"
    return len(self['content'])
```
Connection setup. The pool object is passed into all the ORM methods, so it's a one-stop shop for switching between test and prod.
```python
pool = pgmock.PgPoolMock()
```
Create table and do an insert.
```python
Model.create_table(pool)
Model.insert_all(pool, 1, 2, 'three')
assert pool.tables['model'].rows == [[1, 2, 'three']] # everything is stored like you'd expect
```
This is a multitenant autoincrement insert:
```python
Model.insert_mtac(pool, {'userid':1}, 'id2', ('content',), ('hello',))
assert pool.tables['model'].rows[1] == [1, 3, 'hello']
# notice that 'id2' is one more than for the previous row
```
Here's an example of querying the SQL engine directly. This code is running in-process without talking to an external database.
```python
assert pool.select('select userid,id2 from model where userid=2-1')==[[1,2],[1,3]]
```

## status

Very very new. Don't use it to run your nuclear facility. Probably don't use the mocking engine as a live database.

SQL is a standard, and many implementations don't replicate the standard exactly. This one also doesn't.

Run `pip install . && py.test` in the root dir to see if pg13 will work on your system. Hint: you can probably survive without the psycopg2 dependency.

Supported SQL features:
* select, insert, update, create table, delete
* scalar subqueries (i.e. `select * from t1 where a=(select b from t2 where c=true)`)
* various join syntax (but without a serious query planner, it's not efficient on large tables)
* sub-selects with alias, i.e. temporary tables in select commands

Missing SQL features:
* common table expressions (`with t0 as (select * from t1 where a=5) select * from t0,t2 where t0.a=t2.a`)
* indexes and constraints (`create index` statements will parse but are a no-op)
* asc and desc keywords in `order by` expressions (asc by default; but you can use a minus sign to simulate desc in some cases)
* drop table
* type checking (i.e. it will let you insert integers in a text column)
