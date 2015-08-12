import pytest
from pg13 import table, sqparse2
from .test_pgmock import prep

def mk_colx(name, type=None):
  "helper; we don't need all the fields"
  return sqparse2.ColX(name, None, None, None, None, None)

@pytest.fixture
def table1():
  cols = [mk_colx('a','int'), mk_colx('b','text')]
  return table.Table('t1', cols, None)

@pytest.fixture
def table2():
  cols = [mk_colx('a','int'), mk_colx('b','text')]
  return table.Table('t2', cols, None)

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

def test_field_default():
  tables, run = prep("create table t1 (i serial, a int default 1, b text default 'whatever')")
  print tables
  print table.field_default(tables['t1'].fields[0], 't1', tables)
  raise NotImplementedError('nextup')

def test_toliteral(): raise NotImplementedError

def test_assemble_pkey(): raise NotImplementedError

def test_table_expand_row(table1):
  assert table1.expand_row(['a'], [1]) == [1, table.Missing]
  assert table1.expand_row(['b','a'], [2, 1]) == [1, 2]

def test_table_to_rowlist(): raise NotImplementedError
def test_table_get_column(): raise NotImplementedError
def test_table_pkey_get(): raise NotImplementedError
def test_table_fix_rowtypes(): raise NotImplementedError
def test_table_apply_defaults(): raise NotImplementedError
def test_table_insert(): raise NotImplementedError
def test_table_match(): raise NotImplementedError
def test_table_lookup(): raise NotImplementedError
def test_table_update(): raise NotImplementedError
def test_table_delete(): raise NotImplementedError
