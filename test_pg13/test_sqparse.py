import pytest
from pg13 import sqparse2,pgmock

def test_parse_arraylit():
  v=sqparse2.ArrayLit((sqparse2.Literal(1),sqparse2.Literal(2),sqparse2.Literal("three")))
  assert v==sqparse2.parse("array[1,2,'three']")
  assert v==sqparse2.parse("{1,2,'three'}")

def test_parse_select():
  # this is also testing nesting and various whatever
  from pg13.sqparse2 import NameX,CommaX,SelectX,Literal,ArrayLit,BinX,UnX,OpX,CallX,FromTableX,AsterX
  selx=sqparse2.parse('select *,coalesce(x+3,0),{4,5},array[1,2],(select i from tbl) from whatever where (z+-1=10) and (y<5.5)')
  assert selx.cols==CommaX((
    AsterX(),
    CallX('coalesce',CommaX([
      BinX(OpX('+'),NameX('x'),Literal(3)),
      Literal(0),
    ])),
    ArrayLit((Literal(4),Literal(5))),
    ArrayLit((Literal(1),Literal(2))),
    SelectX(CommaX([NameX('i')]),[FromTableX('tbl',None)],None,None,None,None),
  ))
  # nested select in comma
  assert selx.tables==[FromTableX('whatever',None)]
  assert selx.where==BinX(
    OpX('and'),
    BinX(
      OpX('='),
      BinX(OpX('+'),NameX('z'),UnX(OpX('-'),Literal(1))),
      Literal(10)
    ),
    BinX(OpX('<'),NameX('y'),Literal(5.5))
  )
  assert selx.limit is None and selx.order is None and selx.offset is None

def test_parse_create():
  # todo: real tests here instead of repr comparison
  from pg13.sqparse2 import Literal,NameX,CreateX,ColX,PKeyX,NullX
  assert sqparse2.parse('create table tbl (a int, b int, c text[])')==CreateX(
    False, 'tbl', [
      ColX('a','int',False,False,None,False),
      ColX('b','int',False,False,None,False),
      ColX('c','text',True,False,None,False),
    ], None
  )
  assert sqparse2.parse('create table tbl (a int, b int, primary key (a,b))')==CreateX(
    False, 'tbl', [
      ColX('a','int',False,False,None,False),
      ColX('b','int',False,False,None,False),
    ], PKeyX(['a','b'])
  )
  ex=sqparse2.parse('create table t1 (a int default 7, b int default null, d int primary key)')
  print ex.cols[0].default, ex.cols[1].default, ex.cols[2].pkey
  assert ex.cols[0].default==Literal(7) and ex.cols[1].default==NullX() and ex.cols[2].pkey
  assert sqparse2.parse('create table t1 (a int not null)').cols[0].not_null
  print sqparse2.parse('create table if not exists t1 (a int not null)')

def test_parse_insert():
  from pg13.sqparse2 import InsertX,NameX,CommaX,Literal,ReturnX,AsterX
  assert sqparse2.parse('insert into t1 (a,b) values (1,2)')==InsertX(
    't1', ['a','b'], [Literal(1),Literal(2)], None
  )
  assert sqparse2.parse('insert into t1 values (1,2)')==InsertX(
    't1', None, [Literal(1),Literal(2)], None
  )
  assert sqparse2.parse('insert into t1 values (1,2) returning *')==InsertX(
    't1', None, [Literal(1),Literal(2)], ReturnX(AsterX())
  )
  assert sqparse2.parse('insert into t1 values (1,2) returning (a,b)')==InsertX(
    't1', None, [Literal(1),Literal(2)], ReturnX(CommaX((NameX('a'),NameX('b'))))
  )

def test_parse_update():
  from pg13.sqparse2 import NameX,AssignX,BinX,OpX,Literal,ReturnX,CommaX
  x=sqparse2.parse('update t1 set a=5,d=x+9 where 35 > 50 returning (a,b+1)')
  assert x.tables==['t1']
  assert x.assigns==[
    AssignX('a',Literal(5)),
    AssignX('d',BinX(OpX('+'),NameX('x'),Literal(9))),
  ]
  assert x.where==BinX(OpX('>'),Literal(35),Literal(50))
  assert x.ret==ReturnX(CommaX((
    NameX('a'),
    BinX(OpX('+'),NameX('b'),Literal(1)),
  )))

def test_strlit():
  from pg13.sqparse2 import Literal
  x=sqparse2.parse("select 'literal1','literal two','literal \\'three\\'' from t1")
  assert x.cols==sqparse2.CommaX((Literal('literal1'),Literal('literal two'),Literal("literal 'three'")))

def test_boolx():
  "small-scale test of boolx parsing"
  from pg13.sqparse2 import Literal,NameX,OpX,BinX,UnX
  assert BinX(OpX('and'),BinX(OpX('<'),NameX('a'),Literal(5)),BinX(OpX('='),NameX('z'),Literal(3)))==sqparse2.parse('a<5 and z=3')
  assert BinX(OpX('<'),NameX('a'),UnX(OpX('-'),Literal(5)))==sqparse2.parse('a<-5')

def is_balanced(binx):
  "helper for test_precedence"
  def unbalanced(outer, inner): return not is_balanced(inner) or inner.op < outer.op
  if isinstance(binx.left, sqparse2.BinX) and unbalanced(binx, binx.left): return False
  if isinstance(binx.right, sqparse2.BinX) and unbalanced(binx, binx.right): return False
  return True
def test_precedence():
  "check order of operations in boolx"
  # this isn't an awesome test; the parser might accidentally get it right. better than nothing.
  assert is_balanced(sqparse2.parse('a+1<5 and z=3 or z=6'))
def test_unary_precedence():
  assert isinstance(sqparse2.parse('select * from t1 where not a=0').where,sqparse2.UnX)
  assert isinstance(sqparse2.parse('select -a+1 from t1').cols.children[0],sqparse2.BinX) # warning: I'm not ensuring this outcome, it just happens to work.

def test_parse_sub():
  assert sqparse2.parse('select * from t1 where x=%s').where.right is sqparse2.SubLit

def test_select_emptywhere():
  with pytest.raises(sqparse2.SQLSyntaxError): sqparse2.parse('select * from t1 where')

def test_multi_stmt():
  "make sure that multi statement strings fail loudly (rather than silently skipping the extras)"
  with pytest.raises(sqparse2.SQLSyntaxError): sqparse2.parse('select * from t1; update t2 set a=3')

def test_case():
  "parse case stmt"
  x=sqparse2.parse('select case when x=3 then 10 when x=4 then 20 else 30 end from t1')
  assert len(x.cols.children)==1 # i.e. make sure the case isn't getting distributed across columns somehow
  casex,=x.cols.children
  assert len(casex.cases)==2
  assert casex.elsex==sqparse2.Literal(30)

def test_parse_tuple_in():
  x=sqparse2.parse('select * from t1 where (a,b) in %s')
  assert isinstance(x.where.left,sqparse2.CommaX)

def test_parse_is_not(): assert sqparse2.parse('select * from t1 where a is not null').where.op.op=='is not'

def test_parse_index():
  stmts=[
    sqparse2.parse('create index on t1 (a,b)'),
    sqparse2.parse('create index on t1 (a,b) where a<30'),
    sqparse2.parse('create index on t1 using gist (a,b) where x=5'),
  ]
  assert all(isinstance(x,sqparse2.IndexX) for x in stmts)

def test_parse_delete():
  from pg13.sqparse2 import NameX,OpX,BinX,Literal
  assert sqparse2.parse('delete from t1 where a=3')==sqparse2.DeleteX(
    't1',
    BinX(OpX('='), NameX('a'), Literal(3)),
    None
  )

def test_attr():
  from pg13.sqparse2 import AttrX,NameX
  assert sqparse2.parse('a.b')==AttrX(NameX('a'),NameX('b'))
  assert sqparse2.parse('a.*')==AttrX(NameX('a'),sqparse2.AsterX())
  with pytest.raises(sqparse2.SQLSyntaxError): sqparse2.parse('a.b.c')

def test_join_syntax():
  from pg13.sqparse2 import JoinX,FromTableX,BinX,OpX,NameX,AttrX,CommaX,AsterX
  ex=sqparse2.parse('select * from t1,t2 where t1.x=t2.y')
  assert all(isinstance(x,sqparse2.AttrX) for x in (ex.where.left,ex.where.right))
  assert sqparse2.parse('select * from t1 join t2').tables==[
    JoinX(FromTableX('t1',None),FromTableX('t2',None),None)
  ]
  assert sqparse2.parse('select * from t1 join t2 on x=y').tables==[JoinX(
    FromTableX('t1',None),
    FromTableX('t2',None),
    BinX(OpX('='), NameX('x'), NameX('y'))
  )]
  x = sqparse2.parse('select t1.* from t1 join t2 on x=y and z=a')
  assert x.cols==CommaX([AttrX(NameX('t1'),AsterX())])
  assert x.tables==[JoinX(
    FromTableX('t1',None),
    FromTableX('t2',None),
    BinX(OpX('and'),BinX(OpX('='),NameX('x'),NameX('y')),BinX(OpX('='),NameX('z'),NameX('a')))
  )]
  assert sqparse2.parse('select t1.*,t2.* from t1 join t2 on x=y').cols==CommaX([
    AttrX(NameX('t1'),AsterX()),
    AttrX(NameX('t2'),AsterX())
  ])
  assert sqparse2.parse('select * from t1 join t2 on t1.x=t2.y').tables==[JoinX(
    FromTableX('t1',None),
    FromTableX('t2',None),
    BinX(OpX('='),AttrX(NameX('t1'),NameX('x')),AttrX(NameX('t2'),NameX('y')))
  )]

def test_xgetset():
  "tree-aware getitem/setitem for expressions"
  from pg13.sqparse2 import NameX,AttrX,FromTableX
  x = sqparse2.parse('select * from t1,t2 where t1.x=t2.y')
  # named get
  assert x['where','left','attr']==NameX('x')
  # named set
  x['where','left','attr'] = 'hello'
  assert x['where','left']==AttrX(NameX('t1'),'hello')
  # index get
  assert x[('tables',0),]==FromTableX('t1',None)
  # index set
  x[('tables',0),] = 'hello'
  assert x.tables[0] == 'hello'
  assert x[('tables',0),] == 'hello'

def test_mult_vs_aster():
  "make sure that asterisk is handled differently from the multiplication operator"
  from pg13.sqparse2 import AsterX,BinX,OpX,NameX
  assert sqparse2.parse('select *,a*b from t1').cols.children==[
    AsterX(),
    BinX(OpX('*'),NameX('a'),NameX('b'))
  ]
