import pytest,lrparsing
from pg13 import sqparse,pgmock

def test_parse_arraylit():
  v=sqparse.ArrayLit((sqparse.Literal(1),sqparse.Literal(2),sqparse.Literal("three")))
  assert v==sqparse.parse("array[1,2,'three']")
  assert v==sqparse.parse("{1,2,'three'}")

def test_parse_select():
  # this is also testing nesting and various whatever
  from pg13.sqparse import NameX,CommaX,SelectX,Literal,ArrayLit,BinX,UnX,OpX,CallX,FromListX,FromTableX
  selx=sqparse.parse('select *,coalesce(x+3,0),{4,5},array[1,2],(select i from tbl) from whatever where (z+-1=10) and (y<5.5)')
  assert selx.cols==CommaX((
    '*',
    CallX(NameX('coalesce'),CommaX((
      BinX(OpX('arith_op','+'),NameX('x'),Literal(3)),
      Literal(0),
    ))),
    ArrayLit((Literal(4),Literal(5))),
    ArrayLit((Literal(1),Literal(2))),
    SelectX(CommaX([NameX('i')]),FromListX([FromTableX('tbl',None)]),None,None,None,None),
  ))
  assert selx.tables==FromListX([FromTableX('whatever',None)])
  assert selx.where==BinX(
    OpX('bool_op','and'),
    BinX(
      OpX('cmp_op','='),
      BinX(OpX('arith_op','+'),NameX('z'),UnX(OpX('arith_op','-'),Literal(1))),
      Literal(10)
    ),
    BinX(OpX('cmp_op','<'),NameX('y'),Literal(5.5))
  )
  assert selx.limit is None and selx.order is None and selx.offset is None

def test_parse_create():
  # todo: real tests here instead of repr comparison
  from pg13.sqparse import Literal,NameX
  assert repr(sqparse.parse('create table tbl (a int, b int, c text[])'))=="CreateX[NameX('tbl'),None](ColX[int,False]('a',default=None,pkey=False,not_null=False),ColX[int,False]('b',default=None,pkey=False,not_null=False),ColX[text,True]('c',default=None,pkey=False,not_null=False))"
  assert repr(sqparse.parse('create table tbl (a int, b int, primary key (a,b))'))=="CreateX[NameX('tbl'),PKeyX(a,b)](ColX[int,False]('a',default=None,pkey=False,not_null=False),ColX[int,False]('b',default=None,pkey=False,not_null=False))"
  ex=sqparse.parse('create table t1 (a int default 7, b int default null, d int primary key)')
  assert ex.cols[0].default==Literal(7) and ex.cols[1].default==NameX('null') and ex.cols[2].pkey
  assert sqparse.parse('create table t1 (a int not null)').cols[0].not_null
  print sqparse.parse('create table if not exists t1 (a int not null)')

def test_parse_insert():
  from pg13.sqparse import InsertX,NameX,CommaX,Literal,ReturnX
  assert sqparse.parse('insert into t1 (a,b) values (1,2)')==InsertX(
    NameX('t1'), CommaX((NameX('a'),NameX('b'))), CommaX((Literal(1),Literal(2))), None
  )
  assert sqparse.parse('insert into t1 values (1,2)')==InsertX(
    NameX('t1'), None, CommaX((Literal(1),Literal(2))), None
  )
  assert sqparse.parse('insert into t1 values (1,2) returning *')==InsertX(
    NameX('t1'), None, CommaX((Literal(1),Literal(2))), ReturnX('*')
  )
  assert sqparse.parse('insert into t1 values (1,2) returning (a,b)')==InsertX(
    NameX('t1'), None, CommaX((Literal(1),Literal(2))), ReturnX(CommaX((NameX('a'),NameX('b'))))
  )

def test_parse_update():
  from pg13.sqparse import NameX,AssignX,BinX,OpX,Literal,ReturnX,CommaX
  x=sqparse.parse('update t1 set a=5,d=x+9 where 35 > 50 returning (a,b+1)')
  assert x.tables.children==[NameX('t1')]
  assert x.assigns.children==[
    AssignX(NameX('a'),Literal(5)),
    AssignX(NameX('d'),BinX(OpX('arith_op','+'),NameX('x'),Literal(9))),
  ]
  assert x.where==BinX(OpX('cmp_op','>'),Literal(35),Literal(50))
  assert x.ret==ReturnX(CommaX((
    NameX('a'),
    BinX(OpX('arith_op','+'),NameX('b'),Literal(1)),
  )))

def test_strlit():
  from pg13.sqparse import Literal
  x=sqparse.parse("select 'literal1','literal two','literal \\'three\\'' from t1")
  assert x.cols==sqparse.CommaX((Literal('literal1'),Literal('literal two'),Literal("literal 'three'")))

def test_boolx():
  "small-scale test of boolx parsing"
  from pg13.sqparse import Literal,NameX,OpX,BinX,UnX
  assert BinX(OpX('bool_op','and'),BinX(OpX('cmp_op','<'),NameX('a'),Literal(5)),BinX(OpX('cmp_op','='),NameX('z'),Literal(3)))==sqparse.parse('a<5 and z=3')
  assert BinX(OpX('cmp_op','<'),NameX('a'),UnX(OpX('arith_op','-'),Literal(5)))==sqparse.parse('a<-5')

def is_balanced(binx):
  "helper for test_precedence"
  def unbalanced(outer, inner): return not is_balanced(inner) or inner.op < outer.op
  if isinstance(binx.left, sqparse.BinX) and unbalanced(binx, binx.left): return False
  if isinstance(binx.right, sqparse.BinX) and unbalanced(binx, binx.right): return False
  return True
def test_precedence():
  "check order of operations in boolx"
  # this isn't an awesome test; the parser might accidentally get it right. better than nothing.
  assert is_balanced(sqparse.parse('a+1<5 and z=3 or z=6'))
def test_unary_precedence():
  assert isinstance(sqparse.parse('select * from t1 where not a=0').where,sqparse.UnX)
  assert isinstance(sqparse.parse('select -a+1 from t1').cols.children[0],sqparse.BinX) # warning: I'm not ensuring this outcome, it just happens to work.

def test_parse_sub():
  assert sqparse.parse('select * from t1 where x=%s').where.right is sqparse.SubLit

def test_select_emptywhere():
  with pytest.raises(sqparse.SQLSyntaxError): sqparse.parse('select * from t1 where')

def test_multi_stmt():
  "make sure that multi statement strings fail loudly (rather than silently skipping the extras)"
  with pytest.raises(lrparsing.TokenError): sqparse.parse('select * from t1; update t2 set a=3')

def test_case():
  "parse case stmt"
  x=sqparse.parse('select case when x=3 then 10 when x=4 then 20 else 30 end from t1')
  assert len(x.cols.children)==1 # i.e. make sure the case isn't getting distributed across columns somehow
  casex,=x.cols.children
  assert len(casex.cases)==2
  assert casex.elsex==sqparse.Literal(30)

def test_parse_tuple_in():
  x=sqparse.parse('select * from t1 where (a,b) in %s')
  assert isinstance(x.where.left,sqparse.CommaX)

def test_parse_is_not(): assert sqparse.parse('select * from t1 where a is not null').where.op.op=='is not'

def test_parse_index():
  stmts=[
    sqparse.parse('create index on t1 (a,b)'),
    sqparse.parse('create index on t1 (a,b) where a<30'),
    sqparse.parse('create index on t1 using gist (a,b) where x=5'),
  ]
  assert all(isinstance(x,sqparse.IndexX) for x in stmts)

def test_parse_delete():
  from pg13.sqparse import NameX,OpX,BinX,Literal
  assert sqparse.parse('delete from t1 where a=3')==sqparse.DeleteX(
    NameX('t1'),
    BinX(OpX('cmp_op','='), NameX('a'), Literal(3))
  )

def test_attr():
  from pg13.sqparse import AttrX,NameX
  assert sqparse.parse('a.b.c')==AttrX(AttrX(NameX('a'),NameX('b')),NameX('c'))

def test_join_syntax():
  from pg13.sqparse import JoinX,FromListX,FromTableX,BinX,OpX,NameX,AttrX,CommaX
  ex=sqparse.parse('select * from t1,t2 where t1.x=t2.y')
  assert all(isinstance(x,sqparse.AttrX) for x in (ex.where.left,ex.where.right))
  assert sqparse.parse('select * from t1 join t2').tables==FromListX([
    JoinX(FromTableX('t1',None),FromTableX('t2',None),None)
  ])
  assert sqparse.parse('select * from t1 join t2 on x=y').tables==FromListX([JoinX(
      FromTableX('t1',None),
      FromTableX('t2',None),
      BinX(OpX('cmp_op','='), NameX('x'), NameX('y'))
  )])
  x = sqparse.parse('select t1.* from t1 join t2 on x=y and z=a')
  assert x.cols==CommaX([AttrX(NameX('t1'),'*')])
  assert x.tables==FromListX([JoinX(
      FromTableX('t1',None),
      FromTableX('t2',None),
      BinX(OpX('bool_op','and'),BinX(OpX('cmp_op','='),NameX('x'),NameX('y')),BinX(OpX('cmp_op','='),NameX('z'),NameX('a')))
    )])
  assert sqparse.parse('select t1.*,t2.* from t1 join t2 on x=y').cols==CommaX([
    AttrX(NameX('t1'),'*'),
    AttrX(NameX('t2'),'*')
  ])
  assert sqparse.parse('select * from t1 join t2 on t1.x=t2.y').tables==FromListX([JoinX(
    FromTableX('t1',None),
    FromTableX('t2',None),
    BinX(OpX('cmp_op','='),AttrX(NameX('t1'),NameX('x')),AttrX(NameX('t2'),NameX('y')))
  )])

def test_xgetset():
  "tree-aware getitem/setitem for expressions"
  from pg13.sqparse import NameX,AttrX,FromTableX
  x = sqparse.parse('select * from t1,t2 where t1.x=t2.y')
  # named get
  assert x['where','left','attr']==NameX('x')
  # named set
  x['where','left','attr'] = 'hello'
  assert x['where','left']==AttrX(NameX('t1'),'hello')
  # index get
  assert x['tables',('fromlist',0)]==FromTableX('t1',None)
  # index set
  x['tables',('fromlist',0)] = 'hello'
  assert x['tables','fromlist'][0]=='hello'
