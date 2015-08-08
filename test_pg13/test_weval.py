"test weval and scope"

import pytest
from pg13 import weval, scope, sqparse2
from .test_pgmock import prep

EXP = sqparse2.parse("select * from t1, (select a as alias from t2 where userid=1) as t_sub")

def test_scope_from_fromx():
  tables, run = prep('create table t1 (a int, b text)')
  run('create table t2 (a int, b text)')
  scope.Scope.from_fromx(tables, EXP.tables)
  run('create table t_sub (a int, b text)')
  with pytest.raises(scope.ScopeCollisionError):
    scope.Scope.from_fromx(tables, EXP.tables)
