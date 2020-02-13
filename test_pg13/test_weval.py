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
  print(weval.classify_wherex(scope_, EXP.tables, EXP.where))
  (_, single), (cart,) = weval.classify_wherex(scope_, EXP.tables, EXP.where)
  assert isinstance(single, weval.SingleTableCond) and single.table == 't1' and isinstance(single.exp, sqparse2.BinX)
  assert isinstance(cart, weval.CartesianCond) and isinstance(cart.exp, sqparse2.BinX)

@pytest.mark.xfail
def test_wherex_to_rowlist():
  tables, run = prep('create table t1 (a int, b text)')
  exp = sqparse2.parse('select * from t1')
  print(weval.wherex_to_rowlist(
    scope.Scope.from_fromx(tables, exp.tables),
    exp.tables,
    exp.where
  ))
  raise NotImplementedError
