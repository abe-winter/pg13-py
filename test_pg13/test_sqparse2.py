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
  assert shortlex('select * from t1 where a=b') == [('NAME','select'), ('OPERATOR','*'), ('NAME','from'), ('NAME','t1'), ('NAME','where'), ('NAME','a'), ('OPERATOR','='), ('NAME','b')]
def test_lex_array():
  assert shortlex('array[1,2,3]')==[('NAME','array'), ('BRACE','['), ('INTLIT','1'), ('SEP',','), ('INTLIT','2'), ('SEP',','), ('INTLIT','3'), ('BRACE',']')]
def test_lex_strlit():
  assert shortlex("'abc def \\'ghi'") == [('STRLIT',"'abc def \\'ghi'")]
def test_lex_float():
  assert shortlex('1.2') == [('INTLIT','1'), ('SEP','.'), ('INTLIT','2')]
