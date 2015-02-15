import pytest
from pg13 import sqparse2
import ply.lex

TOK_ATTRS = ('type','value','lineno','lexpos')
def mktok(tpname,tokval,a,b):
  tok = ply.lex.LexToken()
  for attr,val in zip(TOK_ATTRS,(tpname,tokval,a,b)): setattr(tok,attr,val)
  return tok
def eqtok(self,other):
  "here's hoping I don't break something by monkey patching"
  return all(getattr(self,attr)==getattr(other,attr) for attr in TOK_ATTRS)

ply.lex.LexToken.__eq__ = eqtok

def test_mktok():
  assert mktok('AC','B',1,2)==mktok('AC','B',1,2)
  assert mktok('A','B',1,2)!=mktok('AC','B',1,2)

def shortlex(string): return [(tok.type,tok.value) for tok in sqparse2.lex(string)]

def test_lex_select():
  assert shortlex('select * from t1 where a=b') == [('NAME','select'), ('*','*'), ('NAME','from'), ('NAME','t1'), ('NAME','where'), ('NAME','a'), ('=','='), ('NAME','b')]
def test_lex_array():
  assert shortlex('array[1,2,3]')==[('KWARRAY','array'), ('[','['), ('INTLIT','1'), (',',','), ('INTLIT','2'), (',',','), ('INTLIT','3'), (']',']')]
def test_lex_strlit():
  assert shortlex("'abc def \\'ghi'") == [('STRLIT',"'abc def \\'ghi'")]
def test_lex_float():
  assert shortlex('1.2') == [('INTLIT','1'), ('.','.'), ('INTLIT','2')]
def test_lex_long_toks():
  assert shortlex('a is not b')==[('NAME','a'),('BOOL','is'),('BOOL','not'),('NAME','b')]
  assert shortlex('a != b')[1]==('CMP','!=')

@pytest.mark.xfail
def test_reentrant_lexing():
  raise NotImplementedError('hmm')

def test_parse_math():
  from pg13.sqparse2 import Literal,OpX,BinX,UnX
  assert sqparse2.yacc('1.5')==Literal(1.5)
  assert sqparse2.yacc('1.5 + 3')==BinX(OpX('+'),Literal(1.5),Literal(3))
def test_parse_array():
  from pg13.sqparse2 import ArrayLit,Literal
  arr = ArrayLit([Literal(1),Literal(2),Literal(3)])
  assert arr==sqparse2.yacc('{1,2,3}')
  assert arr==sqparse2.yacc('array[1,2,3]')

@pytest.mark.xfail
def test_operator_order():
  raise NotImplementedError