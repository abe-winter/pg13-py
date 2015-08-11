import pytest
from pg13 import table, sqparse2

def mk_colx(name):
  "helper; we don't need all the fields"
  return sqparse2.ColX(name, None, None, None, None, None)

@pytest.fixture
def composite_row():
  return table.Row(
    table.RowSource(table.Composite, None),
    [
      table.Row(
        table.RowSource(table.Table('t%i' % i, [mk_colx('a')], None), 0),
        (i,)
      )
      for i in range(2)
    ]
  )

@pytest.fixture
def table_():
  cols = [
    sqparse2.ColX('a', 'int', None, None, None, None),
    sqparse2.ColX('b', 'text', None, None, None, None),
  ]
  return table.Table('t1', cols, None)

def test_row_get_table(composite_row):
  assert composite_row.get_table('t0').source.name == 't0'
  assert composite_row.get_table('t1').source.name == 't1'
  assert composite_row.get_table('whatever') is None

def test_table_expand_row(table_):
  assert table_.expand_row(['a'], [1]) == [1, table.Missing]
  assert table_.expand_row(['b','a'], [2, 1]) == [1, 2]

def test_row_gettable(): raise NotImplementedError
def test_row_index(): raise NotImplementedError
def test_row_getitem(): raise NotImplementedError
def test_emergency_cast(): raise NotImplementedError
def test_field_default(): raise NotImplementedError
def test_toliteral(): raise NotImplementedError

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
