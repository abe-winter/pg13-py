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

def test_row_get_table(composite_row):
  assert composite_row.get_table('t0').source.name == 't0'
  assert composite_row.get_table('t1').source.name == 't1'
  assert composite_row.get_table('whatever') is None
