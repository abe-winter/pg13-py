import pytest
from pg13 import table, sqparse2
from .test_pgmock import prep

@pytest.fixture
def table1():
  table1 = table.Table.create(sqparse2.parse('create table t1 (a int, b text primary key)'))
  table1.rows = [[1,'one'],[2,'two'],[3,'three']]
  return table1

@pytest.fixture
def table2():
  return table.Table.create(sqparse2.parse('create table t2 (a int, b text)'))

@pytest.fixture
def composite_row(table1, table2):
  return table.Row(
    table.RowSource(table.Composite, None),
    [
      table.Row(
        table.RowSource(table_, 0),
        (1, str(1))
      )
      for table_ in (table1, table2)
    ]
  )

def test_row_get_table(composite_row):
  assert composite_row.get_table('t1').source.name == 't1'
  assert composite_row.get_table('t2').source.name == 't2'
  assert composite_row.get_table('whatever') is None
  assert composite_row.get_table('t1').get_table('t1') == composite_row.get_table('t1')
  assert composite_row.get_table('t1').get_table('t2') is None

def test_row_index(composite_row):
  with pytest.raises(TypeError):
    composite_row.index('a')
  assert composite_row.get_table('t1').index('a') == 0
  assert composite_row.get_table('t1').index('b') == 1
  with pytest.raises(ValueError):
    assert composite_row.get_table('t1').index('c')

def test_row_getitem(composite_row):
  assert composite_row['t1', 'a'] == 1
  with pytest.raises(table.UnkTableError):
    composite_row['t3', 'a']

@pytest.mark.xfail
def test_emergency_cast():
  "don't write this -- instead, refactor emergency_cast to a real types system"
  raise NotImplementedError

@pytest.mark.xfail
def test_field_default():
  # move to commands
  tables, run = prep("create table t1 (i serial, a int default 1, b text default 'whatever')")
  print tables
  print table.field_default(tables['t1'].fields[0], 't1', tables)
  raise NotImplementedError('nextup')

def test_toliteral():
  assert [None, 1, 's', [1, 2, 3], table.Missing] == map(table.toliteral, [
    sqparse2.NameX('null'),
    sqparse2.Literal(1),
    sqparse2.Literal('s'),
    sqparse2.ArrayLit((1,2,3)),
    table.Missing,
  ])

def test_assemble_pkey():
  exp = sqparse2.parse('create table t1 (a int, b text)')
  assert [] == table.assemble_pkey(exp)
  exp = sqparse2.parse('create table t1 (a int primary key, b text)')
  assert ['a'] == table.assemble_pkey(exp)
  # todo(spec): is this one allowed?
  exp = sqparse2.parse('create table t1 (a int primary key, b text primary key)')
  assert ['a','b'] == table.assemble_pkey(exp)
  exp = sqparse2.parse('create table t1 (a int, b text, primary key (a,b))')
  assert ['a','b'] == table.assemble_pkey(exp)

def test_table_expand_row(table1):
  assert table1.expand_row(['a'], [1]) == [1, table.Missing]
  assert table1.expand_row(['b','a'], [2, 1]) == [1, 2]

def test_table_to_rowlist(table1):
  rows = table1.to_rowlist()
  assert [row.vals for row in rows] == table1.rows
  assert all(row.source.table is table1 for row in rows)
  assert all(i==row.source.index for i, row in enumerate(rows))

def test_table_get_column(table1):
  with pytest.raises(KeyError):
    table1.get_column('c')
  assert table1.get_column('b').name == 'b'

def test_table_pkey_get(table1):
  assert None is table1.pkey_get((1,'missing'))
  assert [3,'three'] == table1.pkey_get((-1,'three'))

def test_table_lookup(table1):
  assert table1.lookup('a').index == 0
  assert table1.lookup('b').index == 1
  with pytest.raises(table.BadFieldName):
    table1.lookup('c')
