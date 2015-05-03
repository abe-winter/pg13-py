import pytest
from pg13 import pgmock,sqparse2,pg,sqex,pgmock_dbapi2

def prep(create_stmt):
  "helper for table setup"
  tables=pgmock.TablesDict()
  tables.apply_sql(sqparse2.parse(create_stmt),(),None)
  def runsql(stmt,vals=()): return tables.apply_sql(sqparse2.parse(stmt),vals,None)
  return tables,runsql

def test_default_null_vs_notprovided():
  "test some none vs missing cases"
  tables,runsql=prep("create table t1 (a int, b int default 7, c int default null)")
  # 1. missing with default null
  runsql("insert into t1 (a,b) values (1,2)")
  # 2. missing triggers default value
  runsql("insert into t1 (a,c) values (1,3)")
  # 3. passed-in None overrides default
  runsql("insert into t1 (a,b) values (1,null)")
  assert tables['t1'].rows==[[1,2,None],[1,7,3],[1,None,None]]

def test_insert():
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  runsql("insert into t1 (a,b,c) values (1,2,3)")
  assert tables['t1'].rows[0]==[1,2,3]

def test_insert_endcols():
  "test len(insert fields)<len(table fields)"
  tables={}
  tables,runsql=prep("create table t1 (a int default null, b int, c int)")
  runsql('insert into t1 (b,c) values (3,4)')
  assert tables['t1'].rows==[[None,3,4]]

def test_insert_dupe():
  tables,runsql=prep("create table t1 (a int, b int, c int, primary key (a,b))")
  runsql("insert into t1 values (1,2,3)")
  with pytest.raises(pg.DupeInsert): runsql("insert into t1 values (1,2,4)")

def test_insert_sub():
  tables,runsql=prep("create table t1 (a int, b int, c int, primary key (a,b))")
  runsql("insert into t1 values (1,2,%s)",(3,))
  assert tables['t1'].rows[0]==[1,2,3]

def test_insert_default():
  tables,runsql=prep("create table t1 (a int, b int, c int default 3)")
  runsql("insert into t1 (a,b) values (1,2)")
  assert tables['t1'].rows[0]==[1,2,3]

def test_insert_returning():
  """raw psycopg2 does this:
  >>> with pool() as con,con.cursor() as cur:
  ...     cur.execute('create temp table t1 (a int, b int, c int)')
  ...     cur.execute('insert into t1 values (1,2,3) returning b')
  ...     print list(cur)
  ...     cur.execute('insert into t1 values (2,2,3) returning *')
  ...     print list(cur)
  ... 
  [(2,)]
  [(2, 2, 3)]
  """
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  assert [[2]]==runsql("insert into t1 values (1,2,3) returning (b)")
  assert [[2,2,3]]==runsql('insert into t1 values (2,2,3) returning *')

def test_select():
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  runsql("insert into t1 values (1,2,3)")
  runsql("insert into t1 values (2,2,3)")
  runsql("insert into t1 values (3,1,3)")
  assert [[1,2,3],[2,2,3]]==runsql("select * from t1 where b=2")

def test_select_some():
  "select columnlist instead of *"
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  runsql("insert into t1 values (1,2,3)")
  assert [[3,1]]==runsql("select c,a from t1")

def test_select_math():
  "unary and binary math & select literal"
  tables,runsql=prep('create table t1(a int)')
  runsql('insert into t1 values(1)')
  assert [[2,-1,5]]==runsql('select a+1,-a,5 from t1')

def test_select_max():
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  assert [None]==runsql("select max(a) from t1")
  runsql("insert into t1 (a,b,c) values (1,2,3)")
  runsql("insert into t1 (a,b,c) values (4,5,6)")
  assert [4]==runsql("select max(a) from t1")
  with pytest.raises(sqparse2.SQLSyntaxError): runsql('select a,max(a) from t1') # todo: spec support

def test_select_coalesce():
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  assert [1]==runsql("select coalesce(max(b),1) from t1")
  for i in range(2): runsql("insert into t1 (a,b,c) values (%s,2,3)",(i,))
  assert [[1],[1]]==runsql("select coalesce(null,1) from t1")

def test_insert_select():
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  runsql("insert into t1 (a,b,c) values (1,2,3)")
  print sqparse2.parse('insert into t1 (a,b,c) values (2,3,(select c from t1 where a=1))')
  runsql("insert into t1 (a,b,c) values (2,3,(select c from t1 where a=1))")
  assert tables['t1'].rows==[[1,2,3],[2,3,3]]

@pytest.mark.xfail
def test_insert_missing_pkey():
  raise NotImplementedError # look up when the spec cares about missing pkey. only when not null specified on the column?
  tables,runsql=prep("create table t1 (a int, b int, c int, primary key (a,b))")
  runsql("insert into t1 (a) values (1)")

def test_create_pkey():
  tables,runsql = prep('create table t1 (a int, b int, primary key (a))')
  with pytest.raises(sqparse2.SQLSyntaxError):
    runsql('create table t2 (a int primary key, b int, primary key (a))')
  assert tables['t1'].pkey == ['a']
  runsql('create table t2 (a int primary key, b int)')
  assert tables['t2'].pkey == ['a']

def test_update():
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  runsql("insert into t1 (a,b,c) values (1,2,3)")
  runsql("insert into t1 (a,b,c) values (2,2,3)")
  runsql("update t1 set b=1,c=2 where a=1")
  assert tables['t1'].rows==[[1,1,2],[2,2,3]]

def test_update_returning():
  tables,runsql=prep('create table t1(a int,b int,c int)')
  runsql('insert into t1 values(1,2,3)')
  assert [[1,3]]==runsql('update t1 set b=5 where a<5 returning a,c')
  assert [[1,3]]==runsql('update t1 set b=5 where a<5 returning (a,c)')
  
  assert [[3]]==runsql('update t1 set b=5 where a<5 returning c') # todo: make sure list of rows is the right return type

def test_in_operator():
  tables,runsql=prep("create table t1 (a int, b int, c int)")
  runsql("insert into t1 (a,b,c) values (1,2,3)")
  runsql("insert into t1 (a,b,c) values (2,2,3)")
  runsql("insert into t1 (a,b,c) values (3,2,3)")
  assert [[1,2,3],[3,2,3]]==runsql("select * from t1 where a in %s",((1,3),))

def test_select_xcomma():
  tables,runsql=prep('create table t1 (a int, b int, c int)')
  tables['t1'].rows=[[1,2,3],[2,3,4]]
  assert [[2,3,4]]==runsql('select * from t1 where (a,b) in %s',([(2,3)],))

def test_not():
  "todo: double-check operator precedence of not vs ="
  tables,runsql=prep("create table t1 (a int,b int)")
  tables['t1'].rows=[[0,0],[1,1]]
  print sqparse2.parse("select * from t1 where not a=0")
  assert [[1,1]]==runsql("select * from t1 where not a=0")

def test_null_handling():
  # https://en.wikipedia.org/wiki/Null_(SQL)#Law_of_the_excluded_fourth_.28in_WHERE_clauses.29
  tables,runsql=prep("create table t1 (a int,b int)")
  tables['t1'].rows=[
    [0,0],
    [0,None],
    [None,None],
  ]
  # these two queries are equivalent
  assert [[0,0],[0,None]]==runsql("select * from t1 where (a=0) or not (a=0)")
  assert [[0,0],[0,None]]==runsql("select * from t1 where a is not null")

  assert [[0,0]]==runsql("select * from t1 where a=b")
  assert [[None,None]]==runsql("select * from t1 where a is null")
  assert []==runsql("select * from t1 where a=null") # I think null=null eval to false or unk or something

def test_case():
  tables,runsql=prep('create table t1 (a int,b int)')
  tables['t1'].rows=[[0,1],[1,2],[2,3]]
  assert [[2],[6],[9]]==runsql('select case when a=0 then 2*b else 3*b end from t1')
  print runsql('select case when a=0 then 2*b end from t1')
  assert [[2],[None],[None]]==runsql('select case when a=0 then 2*b end from t1') # i.e. missing else returns null

def test_array_ops():
  tables,runsql=prep('create table t1 (a int,b int[])')
  runsql('insert into t1 values(8,{1,2,3})')
  runsql('insert into t1 values(9,%s)',([4,5,6],))
  runsql('insert into t1 values(10,array[1,2,3])')
  assert tables['t1'].rows==[[8,[1,2,3]],[9,[4,5,6]],[10,[1,2,3]]]
  assert [[True],[False],[True]]==runsql('select b@>array[1] from t1')
  assert [[[1,2,3,1]],[[4,5,6,1]],[[1,2,3,1]]]==runsql('select b||array[1] from t1')

def test_select_order():
  # todo: asc/desc, test more complicated expressions
  tables,runsql=prep('create table t1 (a int,b int)')
  tables['t1'].rows=[[i,0] for i in range(10,0,-1)]
  print sqparse2.parse('select * from t1 order by a')
  rows=runsql('select * from t1 order by a')
  print 'tso',rows
  assert rows==sorted(rows)

def setup_join_test():
  tables,runsql=prep('create table t1 (a int,b int)')
  runsql('create table t2 (c int, d int)')
  tables['t1'].rows=[[1,2],[3,4]]
  tables['t2'].rows=[[1,3],[2,5]]
  return tables,runsql

def test_join_on():
  tables,runsql = setup_join_test()
  assert [[1,2,1,3]] == runsql('select * from t1 join t2 on a=c')

def test_implicit_join():
  tables,runsql = setup_join_test()
  # todo: make sure real SQL behaves this way
  assert [[1,2,1,3]]==runsql('select * from t1,t2 where a=c')

def test_table_as():
  tables,runsql = setup_join_test()
  assert [[1,2],[3,4]]==runsql('select * from t1 as t')
  # todo below: make sure this is what real SQL does. or more generally, run all tests against postgres as well as pgmock.
  assert [[1,2,1,3]]==runsql('select * from t1 as t,t2 where t.a=t2.c')

def test_join_attr():
  tables,runsql = setup_join_test()
  with pytest.raises(pgmock.BadFieldName): runsql('select t1.* from t1 join t2 on t1.a=t2.a') # todo: this deserves its own test
  assert [[1,2]]==runsql('select t1.* from t1 join t2 on t1.a=t2.c')

# todo: test_name_indexer should be in test_sqex except for reliance on tables_dict. move Table to its own file.
def test_name_indexer():
  from pg13.sqparse2 import NameX,AttrX,AsterX
  x = sqparse2.parse('select * from t1, t2 as alias')
  ni = sqex.NameIndexer.ctor_fromlist(x.tables)
  assert ni.table_order==['t1','t2']
  tables,runsql=prep('create table t1 (a int,b int)')
  runsql('create table t2 (a int,c int)')
  ni.resolve_aonly(tables,pgmock.Table)
  assert (0,1)==ni.index_tuple(tables,'b',False)
  assert (0,1)==ni.index_tuple(tables,sqparse2.NameX('b'),False) # make sure NameX handling works
  assert (1,1)==ni.index_tuple(tables,'c',False)
  with pytest.raises(sqex.ColumnNameError): ni.index_tuple(tables,'a',False)
  assert (0, 0) == ni.index_tuple(tables,AttrX(NameX('t1'),'a'),False)
  assert (1, 0) == ni.index_tuple(tables,AttrX(NameX('alias'),'a'),False)
  assert (1,) == ni.index_tuple(tables,AttrX(NameX('alias'),AsterX()),False)
  assert (0,) == ni.index_tuple(tables,AttrX(NameX('t1'),AsterX()),False)
  assert (1,) == ni.index_tuple(tables,AttrX(NameX('t2'),AsterX()),False)
  with pytest.raises(sqex.TableNameError): ni.index_tuple(tables,sqparse2.AttrX(NameX('bad_alias'),'e'),False)
  with pytest.raises(ValueError): ni.index_tuple(tables,sqparse2.AttrX(NameX('t2'),AsterX()),True)

def test_nested_select():
  "nested select has cardinality issues; add cases as they come up"
  tables,runsql=prep('create table t1 (a int, b int)')
  runsql('create table t2 (a int, b int)')
  tables['t1'].rows=[[0,1],[1,2],[3,4],[8,9]]
  tables['t2'].rows=[[0,1],[0,3],[0,5],[6,1]]
  assert [[1,2]]==runsql('select * from t1 where a=(select b from t2 where a=6)')
  assert []==runsql('select * from t1 where a=(select b from t2 where a=7)')

def test_alias_only():
  tables,runsql=prep('create table t1 (a int, b int)')
  tables['t1'].rows=[[0,0],[1,1],[2,2]]
  assert [[0,0],[1,1]]==runsql('select * from (select * from t1 where a < 2) as sub')
  assert [[0],[1]]==runsql('select a from (select * from t1 where a < 2) as sub')
  assert [[0],[1]]==runsql('select * from (select a from t1 where a < 2) as sub')
  assert [[0],[1]]==runsql('select a from (select a from t1 where a < 2) as sub')
  with pytest.raises(sqex.ColumnNameError): runsql('select b from (select a from t1 where a < 2) as sub')
  with pytest.raises(sqex.ColumnNameError): runsql('select c from (select a from t1 where a < 2) as sub')
  with pytest.raises(sqex.ColumnNameError): runsql('select c from (select * from t1 where a < 2) as sub')
  runsql('create table t2 (a int, c int)')
  tables['t2'].rows=[[0,0]]
  with pytest.raises(sqex.ColumnNameError): runsql('select a,c from (select * from t1) as sub1,(select * from t2) as sub2')
  assert [[0,0],[1,0],[2,0]]==runsql('select b,c from (select * from t1) as sub1,(select * from t2) as sub2')

def test_call_as():
  tables,runsql=prep('create table t1 (a int, b int)')
  tables['t1'].rows=[[None,1],[1,2]]
  assert [[0],[1]]==runsql('select coalesce(a,0) as c from t1')
  assert [[0],[1]]==runsql('select c from (select coalesce(a,0) as c from t1) as sub')

@pytest.mark.xfail # delete without a where clause is broken
def test_delete():
  tables,runsql=prep('create table t1 (a int, b int)')
  tables['t1'].rows=[[0,1],[1,1],[2,0],[2,1]]
  runsql('delete from t1 where b=1')
  assert tables['t1'].rows==[[2,0]]
  runsql('delete from t1')
  assert tables['t1'].rows==[]

def test_unnest():
  tables,runsql=prep('create table t1 (a int, b int[])')
  tables['t1'].rows=[[0,[1,2,3]]]
  assert [[1],[2],[3]]==runsql('select unnest(b) from t1')
  assert [[0,1],[0,2],[0,3]]==runsql('select a,unnest(b) from t1')
@pytest.mark.xfail
def test_max_unnest():
  "more generally, this is testing produces_rows inside consumes_rows"
  tables,runsql=prep('create table t1 (a int, b int[])')
  tables['t1'].rows=[[0,[1,2,3]]]
  assert [3]==runsql('select max(unnest(b)) from t1')

def test_groupby():
  tables,runsql=prep('create table t1 (a int, b int)')
  tables['t1'].rows=[[0,1],[0,2],[1,3],[1,4]]
  assert [[0,2,2],[1,2,4]]==runsql('select a,count(a),max(b) from t1 group by a')
  assert [[0,2,1],[1,2,3]]==runsql('select a,count(a),min(b) from t1 group by a')
  assert [[0,2],[1,2]]==runsql('select a,count(*) from t1 group by a')

def test_textsearch():
  tables,runsql=prep('create table t1 (a int, b text)')
  tables['t1'].rows=[[0,'one two three okay'],[1,'four five six okay']]
  assert []==runsql('select a from t1 where to_tsvector(b) @@ to_tsquery(%s)',('unk_token',))
  assert [[0]]==runsql('select a from t1 where to_tsvector(b) @@ to_tsquery(%s)',('one',))
  assert [[1]]==runsql('select a from t1 where to_tsvector(b) @@ to_tsquery(%s)',('four',))
  assert [[0],[1]]==runsql('select a from t1 where to_tsvector(b) @@ to_tsquery(%s)',('okay',))

def test_serial():
  "make sure default does the right thing for serial column type"
  tables,runsql=prep('create table t1 (a serial, b int)')
  for i in range(3):
    runsql('insert into t1 (b) values (%s)',(i,))
  assert tables['t1'].rows == [[0,0],[1,1],[2,2]]
  # warning: what's supposed to happen when a value is passed for serial?

def test_cast():
  tables,runsql=prep('create table t1 (a int, b text)')
  # cast existing column
  runsql('insert into t1 (a) values (1)')
  runsql('update t1 set b=a::text')
  assert tables['t1'].rows[0] == [1,'1']
  # cast literal
  runsql('insert into t1 values (2, 345::text)')
  assert tables['t1'].rows[1] == [2,'345']

def test_default():
  tables, runsql = prep('create table t1(a int, b boolean default false)')
  runsql('insert into t1 (a) values (0)')
  assert tables['t1'].rows == [[0, False]]

def test_tempkeys():
  td = pgmock.TablesDict()
  td['a'] = [1,2,3]
  with td.tempkeys():
    td['b'] = td['a']
    td['b'].append(4)
    assert td['b'] is td['a']
  assert td['a'] == [1,2,3,4]
  with pytest.raises(KeyError): td['b']

def test_transaction_basics():
  ppm = pgmock_dbapi2.PgPoolMock()
  # 1. test that create table persists past commit
  with ppm.withcur() as cursor:
    cursor.execute('create table t1 (a int, b int)')
  assert ppm.tables.keys() == ['t1']
  # 2. test that insert persists past commit
  with ppm.withcur() as cursor:
    cursor.execute('insert into t1 values (1,3)')
  assert len(ppm.tables['t1'].rows) == 1
  class IgnorableError(StandardError): pass
  # 3. test that create table doesn't survive a rollback
  try:
    with ppm.withcur() as cursor:
      cursor.execute('create table t2 (a int, b int)')
      raise IgnorableError
  except IgnorableError: pass
  assert ppm.tables.keys() == ['t1']
  # 4. test that insert doesn't survive a rollback
  try:
    with ppm.withcur() as cursor:
      cursor.execute('insert into t1 values (1,4)')
      raise IgnorableError
  except IgnorableError: pass
  assert len(ppm.tables['t1'].rows) == 1

def test_create_nexists():
  # 1. create if not exists, table exists
  ppm = pgmock_dbapi2.PgPoolMock()
  with ppm.withcur() as cursor:
    cursor.execute('create table t1 (a int)')
    cursor.execute('create table if not exists t1 (a int, b int)')
    assert len(ppm.tables['t1'].fields) == 1
  # 2. create if not exists, table doesn't exist
  ppm = pgmock_dbapi2.PgPoolMock()
  with ppm.withcur() as cursor:
    cursor.execute('create table if not exists t1 (a int)')
    assert 't1' in ppm.tables
  # 3. create, table exists
  with pgmock_dbapi2.PgPoolMock().withcur() as cursor:
    cursor.execute('create table t1 (a int)')
    with pytest.raises(ValueError) as e:
      cursor.execute('create table t1 (a int, b int)')
    assert e.value.args == ('table_exists','t1')

def test_drop():
  ppm = pgmock_dbapi2.PgPoolMock()
  with ppm.withcur() as cursor:
    cursor.execute('create table t1 (a int)')
    assert 't1' in ppm.tables
    cursor.execute('drop table t1')
    assert 't1' not in ppm.tables
  with pgmock_dbapi2.PgPoolMock().withcur() as cursor:
    cursor.execute('drop table if exists t1')
    with pytest.raises(KeyError) as e:
      cursor.execute('drop table t1')
    assert e.value.args == ('t1',)

def test_create_inherit():
  ppm = pgmock_dbapi2.PgPoolMock()
  with ppm.withcur() as cursor:
    cursor.execute('create table t1 (a int)')
    cursor.execute('create table t1a inherits (t1)')
    assert ppm.tables['t1'].child_tables == [ppm.tables['t1a']]
    assert ppm.tables['t1a'].parent_table == ppm.tables['t1']

def test_drop_inherit():
  ppm = pgmock_dbapi2.PgPoolMock()
  with ppm.withcur() as cursor:
    cursor.execute('create table t1 (a int)')
    cursor.execute('create table t1a inherits (t1)')
    with pytest.raises(pgmock.IntegrityError) as e: # drop parent fails without cascade
      cursor.execute('drop table t1')
    assert 't1a' in ppm.tables and 't1' in ppm.tables
    cursor.execute('drop table t1 cascade') # drop succeeds with cascade
    assert 't1a' not in ppm.tables and 't1' not in ppm.tables
  ppm = pgmock_dbapi2.PgPoolMock()
  with ppm.withcur() as cursor:
    cursor.execute('create table t1 (a int)')
    cursor.execute('create table t1a inherits (t1)')
    cursor.execute('drop table t1a')
    assert 't1a' not in ppm.tables and 't1' in ppm.tables
  # todo: multi-level inherit

@pytest.mark.xfail
def test_drop_fkey_cascade():
  raise NotImplementedError
