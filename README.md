# pg13 [![Build Status](https://travis-ci.org/abe-winter/pg13-py.svg?branch=master)](https://travis-ci.org/abe-winter/pg13-py)

- [intro](#intro)
- [examples](#examples)
- [status](#status)
- [sql implementation](#pure-python-implementation-of-sql)

**install** with `pip install pg13[psyco]`

**docs** at http://pg13.readthedocs.org/en/latest/

## intro

pg13 is an in-memory SQL engine for python designed for fast & isolated tests (i.e. each test gets its own database).

There are three ways to use pg13 in your code:
* normal DBAPI
* pg13 implements a SQLAlchemy dialect. For now, it's incomplete and not well-tested.
* there's a small built-in ORM in the pg13 library

You should consider using pg13 in your own software if:
* you spend a lot of time manually specifying the behavior of database mocks
* your test suite interacts with an external DB and that causes problems
* your database layer has no tests because integration environments are too painful to set up
* you need SQL semantics on top of non-sql data (i.e. python lists) (note: sqlite has a foreign data plugin that will also do this)

But beware: this is alpha software.

Drop me a line if you're using the library. `@gmail: awinter.public`

## examples

Note: everything below is happening in-python and in-memory. Each instance (table dictionary) is completely isolated so your tests can run in parallel or whatever, you don't need a live DB on your system. Interacting with a live database looks exactly the same as below except for creating the pool and the pool.tables lines.
```python
# create a model
from pg13 import pg, pgmock_dbapi2
class Model(pg.Row):
  FIELDS = [('userid','int'),('id2','int'),('content','text')]
  PKEY = 'userid,id2'
  TABLE = 'model'
```
Connection setup. The pool object is passed into all the ORM methods, so it's a one-stop shop for switching between test and prod.
```python
pool = pgmock_dbapi2.PgPoolMock()
```
Create table and do an insert.
```python
Model.create_table(pool)
Model.insert_all(pool, 1, 2, 'three')
# everything is stored like you'd expect:
assert pool.tables['model'].rows == [[1, 2, 'three']]
```
Here's an example of querying the SQL engine directly. This code is running in-process without talking to an external database.
```python
with pool() as dbcon, dbcon.cursor() as cur:
  cur.execute('select userid,id2 from model where userid=2-1')
  assert cur.fetchall()==[[1,2]]
```

**todo:** SQLAlchemy example

## status

This is alpha software. That means you **should** use it for these reasons:
 1. you're interested in contributing to the library
 1. your organization can benefit from a tool like this and has enough developers to add features where needed
 1. your existing test suite is so slow that people aren't running it

But you **shouldn't** use it for these reasons:
 1. don't expect the software to be complete or reliable
 1. don't expect it to accurately mimic your prod SQL server in all cases
 1. don't expect it to 'just work'
 1. you rely on complicated join behavior

SQL is a standard, and many implementations don't replicate the standard exactly. This one also doesn't.

Run `pip install . && py.test` in the root dir to see if pg13 will work on your system.

Supported SQL features:
* commands: select, insert, update, create/drop table, delete (with syntax limitations)
* scalar subqueries (i.e. `select * from t1 where a=(select b from t2 where c=true)`)
* various join syntax (but without a serious query planner, it's not efficient on large tables)
* sub-selects with alias, i.e. temporary tables in select commands
* group by seems to work in simple cases, expect bugs
* some array functions (including unnest) and operators
* text search support is limited (limited versions of to_tsvector, to_tsquery, @@)
* serial columns
* :: casting operator (not all types supported)
* transactions exist but are very crude. in theory they're thread-safe but that's not tested. locking is database-level (i.e. no reading from one table while mutating another). pg13 will do a rollback when there's an error. transactions copy the whole DB, so there may be performance issues for large DBs.
* transactional DDL; create/drop statements are isolated and can be rolled back

Missing SQL features:
* alter table
* common table expressions (`with t0 as (select * from t1 where a=5) select * from t0,t2 where t0.a=t2.a`)
* indexes and constraints (`create index` statements will parse but are a no-op)
* asc and desc keywords in `order by` expressions (asc by default; but you can use a minus sign to simulate desc in some cases)
* type checking (a correct simulation of unicode quirks is particularly lacking)
* lots of functions and operators
* partitioning
* window functions
* anything unique to oracle or mysql
* datetime type & interval math are not supported in syntax. if you pass python datetimes as subbed literals it might work.
* stored procs
* prepared statements

## pure-python implementation of SQL

If you're looking for a pure-python SQL engine (an evaluator, not just a parser), you may be in the right place. pg13's SQL logic weighs in around 1000 lines (600 logic + 350 parser).

See also:
* http://gadfly.sourceforge.net/gadfly.html
* https://pypi.python.org/pypi/engine
* https://github.com/itissid/pypgTAP
* cautionary note: [don't test with sqlite when you use postgres](http://michael.robellard.com/2015/07/dont-test-with-sqllite-when-you-use.html)
* [llamadb, a sql engine in rust](https://github.com/nukep/llamadb)
