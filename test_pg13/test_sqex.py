import pytest
from pg13 import sqex,pgmock,sqparse

def test_sub_arraylit():
  from pg13.sqparse import ArrayLit,Literal,SubLit
  arlit=ArrayLit([Literal('a'),SubLit,Literal('b')])
  (parent,setter),=sqex.sub_slots(arlit)
  assert parent is arlit
  setter(Literal('hello'))
  assert arlit.vals==[Literal('a'),Literal('hello'),Literal('b')] # this is checking that the setter closure didn't capture the end of the loop
  # todo: test recursion *into* array

def test_sub_assignx():
  # todo: test the rest of the SUBSLOT_ATTRS classes
  from pg13.sqparse import SubLit,AssignX,Literal
  asx=AssignX(None,SubLit)
  (parent,setter),=sqex.sub_slots(asx)
  assert parent is asx
  setter(Literal('hello'))
  assert asx.expr==Literal('hello')

def test_sub_stmt():
  from pg13.sqparse import Literal,CommaX
  xsel=sqparse.parse('select *,z-%s from t1 where x=%s')
  (p1,s1),(p2,s2)=sqex.sub_slots(xsel)
  s1(Literal(10))
  s2(Literal(10))
  assert p1.right==Literal(10) and p2.right==Literal(10)
  xins=sqparse.parse('insert into t1 values (%s,%s)')
  (p1,s1),(p2,s2)=sqex.sub_slots(xins)
  s1(Literal('a'))
  s2(Literal('b'))
  assert p1 is p2 and p1==CommaX((Literal('a'),Literal('b')))

def test_dfs():
  from pg13.sqparse import Literal,ArrayLit
  with pytest.raises(ValueError):
    sqex.depth_first_sub(sqparse.parse('select * from t1 where x=%s'), (10,[1,2]))
  xsel = sqex.depth_first_sub(sqparse.parse('select a+%s from t1 where x=%s'), (10,[1,2]))
  assert xsel.cols.children[0].right==Literal(10)
  assert xsel.where.right==ArrayLit((1,2))

def test_decompose_select():
  raise NotImplementedError
