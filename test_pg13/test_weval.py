import pytest
from pg13 import weval, sqparse2, scope, treepath
from .test_pgmock import prep

EXP = sqparse2.parse("select * from t1, (select a as alias from t2 where userid=1) as t_sub where a = alias and t1.a = 0")

def test_flatten_tree():
  # todo: move to test_treepath
  from pg13.sqparse2 import BinX, NameX, OpX, Literal
  exp = sqparse2.parse('select * from t where a and b = 1 and d').where
  def test(exp):
    return isinstance(exp, sqparse2.BinX) and exp.op.op == 'and'
  def enumerator(exp):
    return [exp.left, exp.right]
  assert treepath.flatten_tree(test, enumerator, exp) == [
    NameX('a'),
    BinX(OpX('='), NameX('b'), Literal(1)),
    NameX('d')
  ]

def test_names_from_exp():
  from pg13.sqparse2 import AttrX, NameX
  exp = sqparse2.parse('select * from t where a and b = t.c and d').where
  assert weval.names_from_exp(exp) == [
    NameX('a'),
    NameX('b'),
    AttrX(NameX('t'), NameX('c')),
    NameX('d')
  ]

def test_classify_wherex():
  # def classify_wherex(scope_, fromx, wherex):
  tables, run = prep('create table t1 (a int, b text)')
  run('create table t2 (a int, b text)')
  scope_ = scope.Scope.from_fromx(tables, EXP.tables)
  names, (single,), (cart,) = weval.classify_wherex(scope_, EXP.tables, EXP.where)
  assert names == {'t1', 't_sub'}
  assert isinstance(single, weval.SingleTableCond) and single.table == 't1' and isinstance(single.exp, sqparse2.BinX)
  assert isinstance(cart, weval.CartesianCond) and isinstance(cart.exp, sqparse2.BinX)

def test_wherex_to_rowlist():
  tables, run = prep('create table t1 (a int)')
  tables['t1'].rows = [[1], [2], [3]]
  exp = sqparse2.parse('select * from t1')
  assert 3 == len(weval.wherex_to_rowlist(
    scope.Scope.from_fromx(tables, exp.tables),
    exp.tables,
    exp.where
  ))
  exp = sqparse2.parse('select * from t1 where a < 3')
  assert 2 == len(weval.wherex_to_rowlist(
    scope.Scope.from_fromx(tables, exp.tables),
    exp.tables,
    exp.where
  ))

def test_wherex_to_rowlist_multi():
  tables, run = prep('create table t1 (a int)')
  run('create table t2 (a int)')
  tables['t1'].rows = [[1], [2], [3]]
  tables['t2'].rows = [[1], [3]]
  exp = sqparse2.parse('select * from t1, t2 where t1.a = t2.a')
  assert 2 == len(weval.wherex_to_rowlist(scope.Scope.from_fromx(tables, exp.tables), exp.tables, exp.where))
  exp = sqparse2.parse('select * from t1 join t2 on t1.a = t2.a')
  assert 2 == len(weval.wherex_to_rowlist(scope.Scope.from_fromx(tables, exp.tables), exp.tables, exp.where))
