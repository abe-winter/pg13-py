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
  assert shortlex('select * from t1 where a=b') == [('kw_select','select'), ('*','*'), ('kw_from','from'), ('NAME','t1'), ('kw_where','where'), ('NAME','a'), ('=','='), ('NAME','b')]
def test_lex_array():
  assert shortlex('array[1,2,3]')==[('kw_array','array'), ('[','['), ('INTLIT','1'), (',',','), ('INTLIT','2'), (',',','), ('INTLIT','3'), (']',']')]
def test_lex_strlit():
  assert shortlex("'abc def \\'ghi'") == [('STRLIT',"'abc def \\'ghi'")]
def test_lex_float():
  assert shortlex('1.2') == [('INTLIT','1'), ('.','.'), ('INTLIT','2')]
def test_lex_long_toks():
  from pg13.sqparse2 import NameX,OpX,BinX
  assert shortlex('a is not b')==[('NAME','a'),('kw_is','is'),('kw_not','not'),('NAME','b')]
  assert sqparse2.parse('a is not b')==BinX(OpX('is not'),NameX('a'),NameX('b'))
  assert shortlex('a != b')[1]==('CMP','!=')
  assert shortlex('a = b')[1]==('=','=')

@pytest.mark.xfail
def test_reentrant_lexing():
  raise NotImplementedError('hmm')

def test_parse_math():
  from pg13.sqparse2 import Literal,OpX,BinX,UnX
  assert sqparse2.parse('1.5')==Literal(1.5)
  assert sqparse2.parse('1.5 + 3')==BinX(OpX('+'),Literal(1.5),Literal(3))
def test_parse_array():
  from pg13.sqparse2 import ArrayLit,Literal
  arr = ArrayLit([Literal(1),Literal(2),Literal(3)])
  assert arr==sqparse2.parse('{1,2,3}')
  assert arr==sqparse2.parse('array[1,2,3]')
def test_parse_case():
  from pg13.sqparse2 import CaseX,WhenX,Literal,BinX,OpX,NameX
  assert sqparse2.parse('case when 1 then 10 else 20 end')==CaseX(
    [WhenX(Literal(1),Literal(10))],
    Literal(20)
  )
  print sqparse2.parse('case when 1 then 10 when x=5 then 11 else 5 end')
  assert sqparse2.parse('case when 1 then 10 when x=5 then 11 else 5 end')==CaseX(
    [WhenX(Literal(1),Literal(10)), WhenX(BinX(OpX('='),NameX('x'),Literal(5)),Literal(11))],
    Literal(5)
  )
  assert sqparse2.parse('case when 1 then 10 end')==CaseX(
    [WhenX(Literal(1),Literal(10))],
    None
  )
def test_parse_attr():
  from pg13.sqparse2 import NameX,AsterX,AttrX
  assert sqparse2.parse('hello.abc')==AttrX(NameX('hello'),NameX('abc'))
  assert sqparse2.parse('hello.*')==AttrX(NameX('hello'),AsterX())
def test_parse_call():
  from pg13.sqparse2 import CallX,Literal,NameX,CommaX
  assert sqparse2.parse('call(1,2,3)')==CallX('call',CommaX([Literal(1), Literal(2), Literal(3)]))
def test_parse_select():
  from pg13.sqparse2 import SelectX,CommaX,AsterX,AliasX,BinX,OpX,NameX
  assert sqparse2.parse('select * from t1 where a=b group by a')==SelectX(
    CommaX([AsterX()]),['t1'],
    BinX(OpX('='),NameX('a'),NameX('b')),
    NameX('a'),
    None,None,None
  )

@pytest.mark.xfail
def test_operator_order():
  raise NotImplementedError

def test_select_from_as():
  fromx=sqparse2.parse("select * from (select a as alias from t1 where userid=1) as sub group by tag").tables[0]
  assert isinstance(fromx,sqparse2.AliasX) and isinstance(fromx.name,sqparse2.SelectX) and fromx.alias=='sub'
def test_call_as():
  from pg13.sqparse2 import AliasX,CallX,CommaX,NameX
  assert sqparse2.parse('select unnest(a) as b from t1').cols.children[0]==AliasX(CallX('unnest',CommaX([NameX('a')])),'b')

def test_cast():
  from pg13.sqparse2 import CommaX,CastX,Literal,TypeX
  assert sqparse2.parse('12345::text') == CastX(Literal(12345),TypeX('text',None))
  # todo: what happens when a more complicated expression is before the cast? read specs and do more tests
  assert sqparse2.parse('cast(12345 as text)') == CastX(Literal(12345), TypeX('text',None))

def test_parse_transactions():
  from pg13.sqparse2 import StartX,CommitX,RollbackX
  assert sqparse2.parse('start transaction') == StartX()
  assert sqparse2.parse('commit') == CommitX()
  assert sqparse2.parse('rollback') == RollbackX()

def test_drop():
  from pg13.sqparse2 import DropX
  assert DropX(False,'t1',False) == sqparse2.parse('drop table t1')
  assert DropX(False,'t1',True) == sqparse2.parse('drop table t1 cascade')
  assert DropX(True,'t1',False) == sqparse2.parse('drop table if exists t1')
