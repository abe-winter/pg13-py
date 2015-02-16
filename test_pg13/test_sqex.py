import pytest
from pg13 import sqex,pgmock,sqparse2

def test_sub_arraylit():
  from pg13.sqparse2 import ArrayLit,Literal,SubLit
  arlit=ArrayLit([Literal('a'),SubLit,Literal('b')])
  path,=sqex.sub_slots(arlit, lambda x:x is sqparse2.SubLit)
  assert path==(('vals',1),)
  arlit[path] = Literal('hello')
  assert arlit.vals==[Literal('a'),Literal('hello'),Literal('b')] # this is checking that the setter closure didn't capture the end of the loop
  # todo: test recursion *into* array

def test_sub_assignx():
  # todo: test the rest of the SUBSLOT_ATTRS classes
  from pg13.sqparse2 import SubLit,AssignX,Literal
  asx=AssignX(None,SubLit)
  path,=sqex.sub_slots(asx, lambda x:x is sqparse2.SubLit)
  assert path==('expr',)
  asx[path] = Literal('hello')
  assert asx.expr==Literal('hello')

def test_sub_stmt():
  # warning: a thorough test of this needs to exercise every syntax type. yikes. test_subslot_classes isn't enough.
  from pg13.sqparse2 import Literal,CommaX
  xsel=sqparse2.parse('select *,z-%s from t1 where x=%s')
  p1,p2=sqex.sub_slots(xsel, lambda x:x is sqparse2.SubLit)
  xsel[p1] = Literal(9)
  xsel[p2] = Literal(10)
  assert xsel.cols.children[1].right==Literal(9) and xsel.where.right==Literal(10)
  xins=sqparse2.parse('insert into t1 values (%s,%s)')
  p1,p2=sqex.sub_slots(xins, lambda x:x is sqparse2.SubLit)
  xins[p1] = Literal('a')
  xins[p2] = Literal('b')
  assert xins.values==[Literal('a'), Literal('b')]
  x2 = sqparse2.parse('coalesce(max(col),0)')
  assert sqex.contains_aggregate(x2) # checking that sub_slots can descends into CallX.args

def test_decompose_select():
  # basics
  nix,where = sqex.decompose_select(sqparse2.parse('select * from t1, t2'))
  assert where ==[] and nix.table_order==['t1','t2']
  # where from 'join on'
  nix,where = sqex.decompose_select(sqparse2.parse('select * from t1 join t2 on x=y'))
  assert nix.table_order==['t1','t2'] and isinstance(where[0],sqparse2.BinX)

def test_dfs():
  from pg13.sqparse2 import Literal,ArrayLit
  with pytest.raises(ValueError):
    sqex.depth_first_sub(sqparse2.parse('select * from t1 where x=%s'), (10,[1,2]))
  xsel = sqex.depth_first_sub(sqparse2.parse('select a+%s from t1 where x=%s'), (10,[1,2]))
  assert xsel.cols.children[0].right==Literal(10)
  assert xsel.where.right==ArrayLit((1,2))
