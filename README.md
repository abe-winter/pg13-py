# pg13 ![travis-ci build](https://travis-ci.org/abe-winter/pg13-py.svg?branch=master)

http://pg13.readthedocs.org/en/latest/

pg13 is a simple SQL-modeling package with simple priorities:
* basic support for multi-tenancy
* built-in mocking **at the SQL level**

The advantage of built-in mocking is that your tests run without any external dependencies so they're fast and repeatable, parallelizable if you want. (big test-suites that used to chat with your DB for ten minutes can run in seconds now). Mocking at the SQL level (instead of just the model object) means that your app code can 'talk past' the model layer where needed and still be testable.

A disadvantage of built-in mocking is that the mocking engine may not act the same as your database. A lot of SQL features aren't supported.

## examples

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
Note: everything below is happening in-python and in-memory. Each instance (table dictionary) is completely isolated so your tests can run in parallel or whatever, you don't need a live DB on your system. Interacting with a live database looks exactly the same as below except for creating the pool.
```python
pool = pgmock.PgPoolMock()
Model.create_table(pool)
Model.insert_all(pool, 1, 2, 'three')
assert pool.tables['model'].rows == [[1, 2, 'three']] # everything is stored like you'd expect
```
This is a multitenant autoincrement insert:
```python
Model.insert_mtac(pool, {'userid':1}, 'id2', ('content',), ('hello',))
assert pool.tables['model'].rows[1] == [1, 3, 'hello'] # notice that 'id2' is one more than for the previous row
```
The mocking engine is reasonably complete and you can query it with SQL.
```python
assert pool.select('select userid,id2 from model where userid=2-1')==[[1,2],[1,3]]
```

## status

Very very new. Don't use it to run your nuclear facility. Don't use it as a live database.

SQL is a standards-based system. No implementations replicate the standard exactly. This one doesn't either.

Run `py.test` after installing to see if pg13 will work on your system.
