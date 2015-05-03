import pytest
from pg13 import pgmock_dbapi2, sqparse2

def test_connection():
  with pgmock_dbapi2.connect() as a, a.cursor() as acur:
    acur.execute('create table t1 (a int)')
    acur.execute('insert into t1 values (1)')
    acur.execute('insert into t1 values (3)')

  # test second connction into same DB
  with pgmock_dbapi2.connect(a.db_id) as b, b.cursor() as bcur:
    bcur.execute('select * from t1')
    assert bcur.fetchall() == [[1],[3]]
  
  # test that new connection *doesn't* share DB
  with pgmock_dbapi2.connect() as c, c.cursor() as ccur:
    with pytest.raises(KeyError):
      ccur.execute('select * from t1')

def test_auto_rollback():
  with pytest.raises(sqparse2.SQLSyntaxError):
    with pgmock_dbapi2.connect() as db, db.cursor() as cur:
      cur.execute('create table t1 (a int)')
      cur.execute('insert into t1 values (1)')
      cur.execute("this one won't parse")
  assert 't1' not in db.db

def test_fetchone():
  with pgmock_dbapi2.connect() as db, db.cursor() as cur:
    cur.execute('create table t1 (a int, b int)')
    db.db['t1'].rows = [[1,2],[2,3],[3,4]]
    cur.execute('select * from t1')
    assert cur.fetchone() == [1,2]
    assert cur.fetchone() == [2,3]

def test_exmany():
  "this is also testing subbed literals, I think"
  vals = [[1,2],[3,4],[5,6]]
  with pgmock_dbapi2.connect() as db, db.cursor() as cur:
    cur.execute('create table t1 (a int, b int)')
    cur.executemany('insert into t1 (a, b) values (%s, %s)', map(tuple, vals))
  assert db.db['t1'].rows == vals

def test_iter():
  with pgmock_dbapi2.connect() as db, db.cursor() as cur:
    cur.execute('create table t1 (a int, b int)')
    db.db['t1'].rows = [[1,2],[3,4],[5,6],[7,8]]
    # first, test whole iteration
    cur.execute('select * from t1')
    assert list(cur) == db.db['t1'].rows
    # now test iteration from middle
    cur.execute('select * from t1')
    assert cur.fetchone() == [1,2]
    assert list(cur) == db.db['t1'].rows[1:]

@pytest.mark.xfail
def test_count_after_fetch():
  # todo: look at spec; what's supposed to happen here
  raise NotImplementedError

@pytest.mark.xfail
def test_cursor_description_select():
  raise NotImplementedError

@pytest.mark.xfail
def test_cursor_description_nonselect():
  raise NotImplementedError
