from pg13 import pgmock_dbapi2

def test_connection():
  with pgmock_dbapi2.connect() as a, a.cursor() as acur:
    acur.execute('create table t1 (a int)')
    acur.execute('insert into t1 values (1)')
    acur.execute('insert into t1 values (3)')
  with pgmock_dbapi2.connect(a.db_id) as b, b.cursor() as bcur:
    bcur.execute('select * from t1')
    assert bcur.fetchall() == [(1,),(3,)]
  with pgmock_dbapi2.connect() as c, c.cursor() as ccur:
    with pytest.raises(ValueError): # not actually a ValueError
      ccur.execute('select * from t1')
