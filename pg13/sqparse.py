"parsing for pgmock"

# todo: be more consistent about CommaX vs raw tuple

# differences vs real SQL:
# 1. sql probably allows 'table' as a table name. I think I'm stricter about keywords (and I don't allow quoting columns)

import lrparsing

# errors
class PgMockError(StandardError): pass
class SQLSyntaxError(PgMockError): "base class for errors during parsing. beware: this gets called during table execution for things a real parser would have caught"

class Literal(object):
  def __init__(self,val): self.val=val
  def __repr__(self): return '%s(%r)'%(self.__class__.__name__,self.val)
  def __eq__(self,other): return type(other) is Literal and self.val==other.val # careful: ArrayLit is an instance of Literal
  def toliteral(self): return self.val
  def treeall(self,f): return f(self)
  def treeany(self,f): return f(self)
class ArrayLit(Literal):
  def __init__(self,vals): self.vals=list(vals) # list so it's settable by sqex.sub_slots
  def __repr__(self): return 'Literal[Array](%r)'%(self.vals,)
  def __eq__(self,other): return isinstance(other,ArrayLit) and self.vals==other.vals
  def toliteral(self): return self.vals
  def treeall(self,f): return all(x.treeall(f) for x in self.vals)
  def treeany(self,f): return any(x.treeany(f) for x in self.vals)
class SubLit(Literal): pass

class BaseX(object):
  "note: expressions could just be dicts, *but* classes save 'what attrs does this have' experimentation runs. may also help the smater (isinstance-aware) linters."
  def __eq__(self,other): raise NotImplementedError('%s.eq'%(self.__class__.__name__)) # to avoid false negatives
  def __repr__(self): return '%s(???)'%(self.__class__.__name__)
  def treeall(self,f): return f(self)
  def treeany(self,f): return f(self)
class NameX(BaseX):
  def __init__(self,name): self.name=name
  def __repr__(self): return 'NameX(%r)'%self.name
  def __eq__(self,other): return isinstance(other,NameX) and self.name==other.name
class OpX(BaseX):
  PRIORITY=('bool_op','cmp_op','arith_op') # todo: make this bare operators instead of type
  def __init__(self,optype,op): self.optype,self.op=optype,op
  def __repr__(self): return 'OpX(%r)'%self.op # optype not necessary because op maps canonically to optype
  def __lt__(self,other):
    if not isinstance(other,OpX): raise TypeError
    return self.PRIORITY.index(self.optype) < self.PRIORITY.index(other.optype)
  def __eq__(self,other): return isinstance(other,OpX) and (self.optype,self.op)==(other.optype,other.op)
class BinX(BaseX):
  def __init__(self,op,left,right): self.op,self.left,self.right=op,left,right
  def __repr__(self): return 'BinX[%r](%r,%r)'%(self.op,self.left,self.right)
  def __eq__(self,other): return isinstance(other,BinX) and (self.op,self.left,self.right)==(other.op,other.left,other.right)
  def treeall(self,f): return f(self) and self.left.treeall(f) and self.right.treeall(f)
  def treeany(self,f): return f(self) or self.left.treeany(f) or self.right.treeany(f)
class UnX(BaseX):
  def __init__(self,op,val): self.op,self.val=op,val
  def __repr__(self): return 'UnX[%r](%r)'%(self.op,self.val)
  def __eq__(self,other): return isinstance(other,UnX) and (self.op,self.val)==(other.op,other.val)
  def treeall(self,f): return f(self) and self.val.treeall(f)
  def treeany(self,f): return f(self) or self.val.treeany(f)
class CommaX(BaseX):
  def __init__(self,children): self.children=list(children) # needs to be a list (i.e. mutable) for SubLit substitution (in sqex.sub_slots)
  def __eq__(self,other): return isinstance(other,CommaX) and self.children==other.children
  def __repr__(self): return 'CommaX(%s)'%','.join(map(repr,self.children))
  def __eq__(self,other): return isinstance(other,CommaX) and all(x==ox for x,ox in zip(self.children,other.children))
  def treeall(self,f): return f(self) and all(x.treeall(f) for x in self.children)
  def treeany(self,f): return f(self) or any(x.treeany(f) for x in self.children)
class CallX(BaseX):
  def __init__(self,f,args): self.f,self.args=f,args # args will be a CommaX
  def __repr__(self): return 'CallX[%r] %r'%(self.f,self.args)
  def __eq__(self,other): return isinstance(other,CallX) and (self.f,self.args)==(other.f,other.args)
  def treeall(self,f): return f(self) and self.args.treeall(f)
  def treeany(self,f): return f(self) or self.args.treeany(f)
class WhenX(BaseX):
  def __init__(self,when,then): self.when,self.then=when,then
  def __repr__(self): return 'WhenX(%r,%r)'%(self.when,self.then)
  def __eq__(self,other): return isinstance(other,WhenX) and (self.when,self.then)==(other.when,other.then)
  def treeall(self,f): return f(self) and self.when.treeall(f) and self.then.treeall(f)
  def treeany(self,f): return f(self) or self.when.treeany(f) or self.then.treeany(f)
class CaseX(BaseX):
  def __init__(self,cases,elsex): self.cases,self.elsex=cases,elsex
  def __repr__(self): return 'CaseX(%r,%r)'%(self.cases,self.elsex)
  def __eq__(self,other): return isinstance(other,CaseX) and (self.cases,self.elsex)==(other.cases,other.elsex)
  def treeall(self,f): return f(self) and all(x.treeall(f) for x in self.cases) and self.elsex.treeall(f)
  def treeany(self,f): return f(self) or any(x.treeany(f) for x in self.cases) or self.elsex.treeany(f)

class CommandX(BaseX): "base class for top-level commands. probably won't ever be used."
class SelectX(CommandX):
  ATTRS=('cols','tables','where','order','limit','offset')
  def __init__(self,cols,tables,where,order,limit,offset): self.cols,self.tables,self.where,self.order,self.limit,self.offset=cols,tables,where,order,limit,offset
  def __eq__(self,other):
    return isinstance(other,SelectX) and all(getattr(self,attr)==getattr(other,attr) for attr in self.ATTRS)
  def __repr__(self): return 'SelectX(%r,%r,%r,%r,%r)'%(self.cols,self.tables,self.where,self.offset,self.limit)
  def treeall(self,f): return f(self) and all(getattr(self,a).treeall(f) for a in self.ATTRS)
  def treeany(self,f): return f(self) or any(getattr(self,a).treeany(f) for a in self.ATTRS)

class ColX(BaseX):
  ATTRS=('name','coltp','isarray','defaeult','pkey','not_null')
  def __init__(self,name,coltp,isarray,default,pkey,not_null): self.name,self.coltp,self.isarray,self.default,self.pkey,self.not_null=name,coltp,isarray,default,pkey,not_null
  def __repr__(self): return 'ColX[%s,%r](%r,default=%r,pkey=%r,not_null=%r)'%(self.coltp.name,self.isarray,self.name.name,self.default,self.pkey,self.not_null)
  def __eq__(self,other): return isinstance(other,ColX) and all(getattr(self,a)==getattr(other,a) for a in self.ATTRS)
class PKeyX(BaseX):
  def __init__(self,fields): self.fields=fields
  def __repr__(self): return 'PKeyX(%s)'%','.join(f.name for f in self.fields)
  def __eq__(self,other): return isinstance(other,PKeyX) and self.fields==other.fields
class CreateX(CommandX):
  def __init__(self,name,cols,pkey): self.name,self.cols,self.pkey=name,cols,pkey
  def __repr__(self): return 'CreateX[%r,%r](%s)'%(self.name,self.pkey,','.join(map(repr,self.cols)))

class ReturnX(BaseX):
  def __init__(self,expr): self.expr=expr
  def __repr__(self): return 'ReturnX(%r)'%self.expr
  def __eq__(self,other): return isinstance(other,ReturnX) and self.expr==other.expr
class InsertX(CommandX):
  def __init__(self,table,cols,values,ret): self.table,self.cols,self.values,self.ret=table,cols,values,ret
  def __repr__(self): return 'InsertX[%r](%r,%r,%r)'%(self.table,self.cols,self.values,self.ret)
  def __eq__(self,other): return isinstance(other,InsertX) and all(getattr(self,k)==getattr(other,k) for k in ('table','cols','values','ret'))

class AssignX(BaseX):
  def __init__(self,col,expr): self.col,self.expr=col,expr
  def __repr__(self): return 'AssignX(%r,%r)'%(self.col,self.expr)
  def __eq__(self,other): return isinstance(other,AssignX) and (self.col,self.expr)==(other.col,other.expr)
class UpdateX(CommandX):
  def __init__(self,tables,assigns,where,ret): self.tables,self.assigns,self.where,self.ret=tables,assigns,where,ret
  def __repr__(self): return 'UpdateX(%r,%r,%r,%r)'%(self.tables,self.assigns,self.where,self.ret)

class IndexX(CommandX):
  def __init__(self,string): self.string=string
  def __repr__(self): return 'IndexX()'

def bin_priority(op,left,right):
  "I don't know how to handle order of operations in the LR grammar, so here it is"
  # note: recursion limits protect this from infinite looping. I'm serious. (i.e. it will crash rather than hanging)
  if isinstance(left,BinX) and left.op < op: return bin_priority(left.op,left.left,bin_priority(op,left.right,right))
  elif isinstance(left,UnX) and left.op < op: return un_priority(left.op,BinX(op,left.val,right)) # note: obviously, no need to do this when right is a UnX
  elif isinstance(right,BinX) and right.op < op: return bin_priority(right.op,bin_priority(op,left,right.left),right.right)
  else: return BinX(op,left,right)
def un_priority(op,val):
  "unary expression order-of-operations helper"
  if isinstance(val,BinX) and val.op < op: return bin_priority(val.op,UnX(op,val.left),val.right)
  else: return UnX(op,val)

def keywordify(kw_order,keywords,vals):
  "helper for getting ordered / null values from expressions that are marked by keyword"
  d=dict(zip(keywords,vals))
  return (d.get(field,None) for field in kw_order)
def kw(name): return lrparsing.Keyword(name,False)
def tup_remove(tup,val):
  if val in tup:
    i=tup.index(val)
    return tup[:i]+tup[i+1:]
  else: return tup

class SQLG(lrparsing.Grammar):
  # todo: do I need to support things like select a.*,b.* from a,b (i.e. star-attribute). if yes, put it in attr, I think
  class T(lrparsing.TokenRegistry):
    # todo: adhere more closely to the spec. http://www.postgresql.org/docs/9.1/static/sql-syntax-lexical.html
    strlit = lrparsing.Token(re="'((?<=\\\\)'|[^'])+'")
    intlit = lrparsing.Token(re='\d+')
    name = lrparsing.Token(re='[A-Za-z]\w*')
    operator = lrparsing.Token(re='\!\=|\|\||@>|[\/\+\-\*\=<>]')
    sublit = lrparsing.Token('%s')
  floatlit = T.intlit + '.' + T.intlit # todo: no whitespace
  token = lrparsing.Prio(T.strlit, floatlit, T.intlit, T.name, T.sublit)
  attr = lrparsing.Prio(lrparsing.THIS, T.name) + '.' + T.name
  call = lrparsing.Prio(attr, T.name) + '(' + lrparsing.Ref('commalist') + ')' # todo: commalist. also, not sure attrs are callable in sql.
  unop = kw('not') | '+' | '-'
  arith_op = lrparsing.Choice(*map(kw,('/','*','+','-')))
  cmp_op = kw('>') | kw('<') | kw('@>') | kw('||') | lrparsing.Prio(kw('!='), kw('='), kw('is') + kw('not'), kw('is')) | kw('in')
  bool_op = kw('or') | kw('and')
  binop = lrparsing.Prio(cmp_op, bool_op, arith_op)
  boolx =  lrparsing.Ref('expr') + binop + lrparsing.Ref('expr') | unop + lrparsing.Ref('expr')
  parenx = '(' + lrparsing.Prio(lrparsing.Ref('selectx'), lrparsing.Ref('expr'), lrparsing.Ref('commalist')) + ')'
  expr = lrparsing.Prio(lrparsing.Ref('case'), boolx, call, attr, token, parenx, lrparsing.Ref('array_ctor'))
  whenx = kw('when') + expr + kw('then') + expr
  case = kw('case') + lrparsing.Repeat(whenx,1) + kw('else') + expr + kw('end')
  commalist = lrparsing.List(expr, ',', 1)
  array_ctor = '{' + lrparsing.Ref('commalist') + '}' | kw('array') + '[' + lrparsing.Ref('commalist') + ']'
  cols_list = lrparsing.List('*' | expr, ',')
  selectx = kw('select') + cols_list + kw('from') + commalist + lrparsing.Opt(kw('where') + expr) + lrparsing.Opt(kw('order') + kw('by') + expr) + lrparsing.Opt(kw('limit') + expr) + lrparsing.Opt(kw('offset') + expr)
  
  # create stmt
  # note below: I'm not supporting 'not null' because lrparsing gets confused by out-of-context 'not'. todo: fix.
  not_null = kw('not') + T.name
  col_spec = T.name + T.name + lrparsing.Opt('[]') + lrparsing.Opt(not_null) + lrparsing.Opt(kw('default') + expr) + lrparsing.Opt(kw('primary') + kw('key'))
  namelist = lrparsing.List(T.name, ',', 1)
  pkey = kw('primary') + kw('key') + '(' + namelist + ')' # todo: are parens required?
  if_nexists = kw('if') + kw('not') + kw('exists')
  createx = kw('create') + kw('table') + lrparsing.Opt(if_nexists) + T.name + '(' + lrparsing.List(col_spec, ',', 1) + lrparsing.Opt(',' + pkey) + ')'
  
  # insert stmt
  returning = kw('returning') + ('*' | commalist | expr)
  insertx = kw('insert') + kw('into') + T.name + lrparsing.Opt('(' + namelist + ')') + kw('values') + '(' + commalist + ')' + lrparsing.Opt(returning)
  
  # update stmt
  assign = (T.name | attr) + kw('=') + expr
  assignlist = lrparsing.List(assign, ',', 1)
  updatex = kw('update') + namelist + kw('set') + assignlist + lrparsing.Opt('where' + expr) + lrparsing.Opt(returning)

  # todo: deletex = 
  START = selectx | createx | insertx | updatex | expr # note: expr doesn't need to be here except that it's useful to run and verify smaller strings

  @classmethod
  def tovalue(clas,x):
    "not as useless as it looks"
    node,value=x[:2]
    if isinstance(node,lrparsing.Token):
      if node is clas.T.name: return NameX(value)
      elif node is clas.T.intlit: return Literal(int(value))
      elif node is clas.T.strlit: return Literal(value[1:-1].replace("\\'","'")) # strip the quotes and replace escapes
      elif node is clas.T.sublit: return SubLit
      else: return value # todo: what ends up here?
    elif node in (clas.token,clas.expr,clas.binop): return value
    elif node is clas.unop: # todo: clean this up and make else case well-defined
      return OpX('bool_op',value) if value=='not' else OpX('arith_op',value)
    elif node in (clas.arith_op,clas.cmp_op,clas.bool_op):
      if len(x)==3:
        if x[1:]!=('is','not'): raise NotImplementedError('expected "is not"',x[1:])
        value='is not'
      return OpX(node.name,value)
    elif node is clas.floatlit: return Literal(float('%i.%i'%(x[-3].val,x[-1].val)))
    elif node is clas.boolx:
      if len(x)==4: left,op,right=x[-3:]; return bin_priority(op,left,right)
      elif len(x)==3: op,val=x[-2:]; return un_priority(op,val)
      else: raise NotImplementedError # shouldn't get here
    elif node in (clas.commalist,clas.cols_list,clas.namelist,clas.assignlist): return CommaX(x[1::2])
    elif node is clas.call: f,_,args,_=x[-4:]; return CallX(f,args)
    elif node is clas.array_ctor: return ArrayLit(x[-2].children)
    elif node is clas.parenx: return x[-2]
    elif node is clas.whenx: return WhenX(x[2],x[4])
    elif node is clas.case: return CaseX(x[2:-3],x[-2])
    elif node is clas.selectx:
      x=tup_remove(x,'by') # ugly
      return SelectX(*keywordify(('select','from','where','order','limit','offset'), x[1::2], x[2::2]))
    elif node is clas.col_spec:
      name,tp=x[1:3]
      isarray=len(x)>3 and x[3]=='[]'
      default=x[x.index('default')+1] if 'default' in x else None # careful: this isn't saying None is the default value (though I think it might be anyway). 'default null' comes through as string 'null'
      not_null='not null' in x
      ispkey=x[-2:]==('primary','key')
      return ColX(name,tp,isarray,default,ispkey,not_null)
    elif node is clas.pkey: return PKeyX(x[-2].children)
    elif node is clas.returning:
      if len(x)==5: return ReturnX(x[-2])
      elif len(x)==3: return ReturnX(x[-1])
      else: raise NotImplementedError # shouldn't get here
    elif node is clas.not_null: return 'not null'
    elif node is clas.if_nexists: return 'if_nexists'
    elif node is clas.createx:
      x=tup_remove(x,'if_nexists')
      name=x[3]; cols=x[5:-1:2]; pkey=None
      if isinstance(cols[-1], PKeyX): cols,pkey=cols[:-1],cols[-1]
      return CreateX(name, cols, pkey)
    elif node is clas.insertx:
      # 'insert', 'into', NameX('t1'), '(', NameX('a'), ',', NameX('b'), ')', 'values', '(', CommaX(1,2), ')'))
      table=x[3]; cols=None; i_values=x.index('values')+1; values_end=x.index(')',i_values)
      if x[4]!='values': cols=x[5]
      vals = x[i_values+1]
      returning = returning=x[-1] if isinstance(x[-1], ReturnX) else None
      return InsertX(table,cols,vals,returning)
    elif node is clas.assign: return AssignX(*x[1::2])
    elif node is clas.updatex:
      ret=x[-1] if isinstance(x[-1],ReturnX) else None
      return UpdateX(*(list(keywordify(('update','set','where'),x[1::2],x[2::2])) + [ret]))
    elif node is clas.START: return value
    else: raise NotImplementedError(node,x)

def parse(s,g=SQLG):
  print s # todo: remove. but for now it's useful; py.test shows this on failures.
  if s.strip().lower().startswith('create index'): return IndexX(s)
  try: return g.parse(s,g.tovalue)
  except lrparsing.ParseError as e:
    print s
    raise SQLSyntaxError(e) # todo: unpack more details from the ParseError (like what expression it thinks it's inside)
