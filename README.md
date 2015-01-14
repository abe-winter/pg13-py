# pg13

pg13 is a simple SQL-modeling package with simple priorities:
* basic support for multi-tenancy
* built-in mocking

The advantage of built-in mocking is that your tests run without any external dependencies so they're fast and repeatable, parallelizable if you want. (big test-suites that used to chat with your DB for ten minutes can run in seconds now). The disadvantage is that the mocking engine may not act the same as your database. A lot of SQL features aren't supported.

The 'pg' doesn't stand for postgres (although postgres is the only platform it's ever been tested on). It's PG-13 like the movie rating (parental guidance suggested). You know how `requests` is 'http for humans'? This is 'orm for adults'.

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

pool = pgmock.PgPoolMock()
# note: everything below is happening in-python and in-memory.
# Each instance is completely isolated so your tests can run in parallel or whatever, you don't need a live DB on your system.
# The calls would look the same (except for creating the pool above) for interacting with a live database.
Model.create_table(pool)
Model.insert_all(pool, 1, 2, 'three')
assert pool.tables['model'].rows == [[1, 2, 'three']] # everything is stored like you'd expect

# this is a multitenant-autoincrement insert
Model.insert_mtac(pool, {'userid':1}, 'id2', ('content',), ('hello',))
assert pool.tables['model'].rows[1] == [1, 3, 'hello'] # notice that 'id2' is one more than for the previous row
```

## status

Very very new. Don't use it to run your nuclear facility. Don't use it as a live database.

SQL is a standards-based system. No implementations replicate the standard exactly. This one doesn't either.

Run `py.test` after installing to see if pg13 will work on your system.
