import pytest
from pg13 import scope, sqparse2
from .test_pgmock import prep

EXP = sqparse2.parse("select * from t1, (select a as alias from t2 where userid=1) as t_sub")

def test_replace_intermediate_types():
  tables, run = prep('create table t1 (a int, b int, c int)')
  run('create table t2 (a int, d int)')
  # single-table expr
  expr = sqparse2.parse('insert into t1 values (2,2,3) returning *')
  scope_ = scope.Scope.from_expr(tables, expr)
  scope_.replace_intermediate_types(expr)
  assert ['t1-a','t1-b','t1-c'] == [ref.display_name for ref in expr.ret.abs_refs]
  # multi-table expr
  expr = sqparse2.parse('select * from t1, t2')
  scope_ = scope.Scope.from_expr(tables, expr)
  scope_.replace_intermediate_types(expr)
  assert ['t1-a', 't1-b', 't1-c', 't2-a', 't2-d'] == [ref.display_name for ref in expr.cols.abs_refs]

def test_nested_asterx():
  """asterx intermediate table trusts the scope for the list of tables.
  make sure there's no inner scope issue (nested selects?)
  """
  raise NotImplementedError

def test_scope_from_fromx():
  tables, run = prep('create table t1 (a int, b text)')
  run('create table t2 (a int, b text)')
  scope.Scope.from_fromx(tables, EXP.tables)
  with pytest.raises(scope.ScopeCollisionError):
    scope.Scope.from_fromx(tables, sqparse2.parse('select * from t1 as t2, t2').tables)

def test_scope_resolve_column():
  from pg13.sqparse2 import NameX, AttrX, AsterX
  tables, run = prep('create table t1 (a int, b text)')
  run('create table t2 (a int, b text)')
  scope_ = scope.Scope.from_fromx(tables, EXP.tables)
  assert scope_.resolve_column(NameX('a')) == ('t1', 'a')
  assert scope_.resolve_column(AttrX(NameX('t1'), NameX('a'))) == ('t1', 'a')
  assert scope_.resolve_column(NameX('alias')) == ('t_sub', 'alias')
  assert scope_.resolve_column(AttrX(NameX('t_sub'), NameX('alias'))) == ('t_sub', 'alias')

@pytest.mark.xfail
def test_scope_resolve_column_asterx():
  raise NotImplementedError('handle AsterX')
