import pytest
from pg13 import weval, sqparse2
from .test_pgmock import prep

def test_flatten_tree():
  from pg13.sqparse2 import BinX, NameX, OpX, Literal
  exp = sqparse2.parse('select * from t where a and b = 1 and d').where
  def test(exp):
    return isinstance(exp, sqparse2.BinX) and exp.op.op == 'and'
  def enumerator(exp):
    return [exp.left, exp.right]
  assert weval.flatten_tree(test, enumerator, exp) == [
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
  raise NotImplementedError

def test_wherex_to_rowlist():
  raise NotImplementedError
