import pytest
from pg13 import scope, sqparse2
from .test_pgmock import prep

EXP = sqparse2.parse("select * from t1, (select a as alias from t2 where userid=1) as t_sub")

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
