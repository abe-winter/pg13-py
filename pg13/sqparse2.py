"parsing for pgmock -- rewritten in PLY"

# todo: be more consistent about CommaX vs raw tuple

# differences vs real SQL:
# 1. sql probably allows 'table' as a table name. I think I'm stricter about keywords (and I don't allow quoting columns)

import ply.lex, ply.yacc

# errors
class PgMockError(StandardError): pass
class SQLSyntaxError(PgMockError): "base class for errors during parsing. beware: this gets called during table execution for things a real parser would have caught"

class BaseX(object):
  "note: expressions could just be dicts, *but* classes save 'what attrs does this have' experimentation runs. may also help the smater (isinstance-aware) linters."
  def __eq__(self,other): raise NotImplementedError('%s.eq'%(self.__class__.__name__)) # to avoid false negatives
  def __repr__(self): return '%s(???)'%(self.__class__.__name__)
  def child(self,index):
    "helper for __getitem__/__setitem__"
    if isinstance(index,tuple):
      attr,i = index
      return getattr(self,attr)[i]
    else: return getattr(self,index)
  def check_i(self,i):
    "helper"
    if not isinstance(i,tuple): raise TypeError('index_by_tuple')
    if not i: raise ValueError('empty_index')
  def __getitem__(self,i):
    self.check_i(i)
    if len(i)==1: return self.child(i[0])
    else: return self.child(i[0])[i[1:]]
  def __setitem__(self,i,x):
    self.check_i(i)
    if len(i)==1:
      if isinstance(i[0],tuple):
        attr,ilist = i[0]
        getattr(self,attr)[ilist] = x
      else: setattr(self,i[0],x)
    else: self.child(i[0])[i[1:]] = x

class Literal(BaseX):
  def __init__(self,val): self.val=val
  def __repr__(self): return '%s(%r)'%(self.__class__.__name__,self.val)
  def __eq__(self,other): return type(other) is Literal and self.val==other.val # careful: ArrayLit is an instance of Literal
  def toliteral(self): return self.val
class ArrayLit(BaseX): # todo: this isn't always a literal. what if there's a select stmt inside? yikes.
  def __init__(self,vals): self.vals=list(vals) # list so it's settable by sqex.sub_slots
  def __repr__(self): return 'Literal[Array](%r)'%(self.vals,)
  def __eq__(self,other): return isinstance(other,ArrayLit) and self.vals==other.vals
  def toliteral(self): return self.vals
class SubLit(object): pass

class NameX(BaseX):
  def __init__(self,name): self.name=name
  def __repr__(self): return 'NameX(%r)'%self.name
  def __eq__(self,other): return isinstance(other,NameX) and self.name==other.name
class AsterX(BaseX):
  def __repr__(self): return 'AsterX()'
  def __eq__(self,other): return isinstance(other, AsterX)
class NullX(BaseX):
  def __repr__(self): return 'NullX()'
  def __eq__(self,other): return isinstance(other,NullX)
class FromTableX(BaseX):
  def __init__(self,name,alias): self.name, self.alias = name, alias
  def __repr__(self): return 'FromTableX(%r,%r)'%(self.name,self.alias)
  def __eq__(self,other): return isinstance(other,FromTableX) and (self.name,self.alias)==(other.name,other.alias)
class JoinX(BaseX):
  def __init__(self,a,b,on_stmt): self.a,self.b,self.on_stmt=a,b,on_stmt
  def __repr__(self): return 'JoinX(%r,%r,%r)'%(self.a,self.b,self.on_stmt)
  def __eq__(self,other): return isinstance(other,JoinX) and (self.a,self.b,self.on_stmt)==(other.a,other.b,other.on_stmt)
class FromListX(BaseX):
  "fromlist is a list of FromTableX | JoinX"
  def __init__(self,fromlist): self.fromlist=list(fromlist)
  def __repr__(self): return 'FromListX(%r)'%self.fromlist
  def __eq__(self,other): return isinstance(other,FromListX) and self.fromlist==other.fromlist
class OpX(BaseX):
  PRIORITY=('or','and','>','<','@>','||','!=','=','is not','is','in','*','/','+','-','not') # not is tight because it's unary when solo
  def __init__(self,op):
    self.op=op
    if op not in self.PRIORITY: raise SQLSyntaxError('unk_op',op)
  def __repr__(self): return 'OpX(%r)'%self.op
  def __lt__(self,other):
    "this is for order of operations"
    if not isinstance(other,OpX): raise TypeError
    return self.PRIORITY.index(self.op) < self.PRIORITY.index(other.op)
  def __eq__(self,other): return isinstance(other,OpX) and self.op==other.op
class BinX(BaseX):
  "binary operator expression"
  def __init__(self,op,left,right): self.op,self.left,self.right=op,left,right
  def __repr__(self): return 'BinX(%r,%r,%r)'%(self.op,self.left,self.right)
  def __eq__(self,other): return isinstance(other,BinX) and (self.op,self.left,self.right)==(other.op,other.left,other.right)
class UnX(BaseX):
  "unary operator expression"
  def __init__(self,op,val): self.op,self.val=op,val
  def __repr__(self): return 'UnX[%r](%r)'%(self.op,self.val)
  def __eq__(self,other): return isinstance(other,UnX) and (self.op,self.val)==(other.op,other.val)
class CommaX(BaseX):
  def __init__(self,children): self.children=list(children) # needs to be a list (i.e. mutable) for SubLit substitution (in sqex.sub_slots)
  def __eq__(self,other): return isinstance(other,CommaX) and self.children==other.children
  def __repr__(self): return 'CommaX(%r)'%(self.children,)
  def __eq__(self,other): return isinstance(other,CommaX) and self.children==other.children
class CallX(BaseX):
  def __init__(self,f,args): self.f,self.args=f,args # args will be a CommaX
  def __repr__(self): return 'CallX[%r] %r'%(self.f,self.args)
  def __eq__(self,other): return isinstance(other,CallX) and (self.f,self.args)==(other.f,other.args)
class WhenX(BaseX):
  def __init__(self,when,then): self.when,self.then=when,then
  def __repr__(self): return 'WhenX(%r,%r)'%(self.when,self.then)
  def __eq__(self,other): return isinstance(other,WhenX) and (self.when,self.then)==(other.when,other.then)
class CaseX(BaseX):
  def __init__(self,cases,elsex): self.cases,self.elsex=cases,elsex
  def __repr__(self): return 'CaseX(%r,%r)'%(self.cases,self.elsex)
  def __eq__(self,other): return isinstance(other,CaseX) and (self.cases,self.elsex)==(other.cases,other.elsex)
class AttrX(BaseX):
  def __init__(self,parent,attr): self.parent,self.attr=parent,attr
  def __repr__(self): return 'AttrX(%r,%r)'%(self.parent,self.attr)
  def __eq__(self,other): return isinstance(other,AttrX) and (self.parent,self.attr)==(other.parent,other.attr)

class CommandX(BaseX): "base class for top-level commands. probably won't ever be used."
class SelectX(CommandX):
  ATTRS=('cols','tables','where','order','limit','offset')
  def __init__(self,cols,tables,where,order,limit,offset): self.cols,self.tables,self.where,self.order,self.limit,self.offset=cols,tables,where,order,limit,offset
  def __eq__(self,other):
    return isinstance(other,SelectX) and all(getattr(self,attr)==getattr(other,attr) for attr in self.ATTRS)
  def __repr__(self): return 'SelectX(%r,%r,%r,%r,%r,%r)'%(self.cols,self.tables,self.where,self.offset,self.limit,self.offset)

class ColX(BaseX):
  ATTRS=('name','coltp','isarray','default','pkey','not_null')
  def __init__(self,name,coltp,isarray,not_null,default,pkey):
    self.name,self.coltp,self.isarray,self.default,self.pkey,self.not_null=name,coltp,isarray,default,pkey,not_null
  def __repr__(self): return 'ColX(%r,%r,%r,not_null=%r,default=%r,pkey=%r)'%(self.name,self.coltp,self.isarray,self.not_null,self.default,self.pkey)
  def __eq__(self,other): return isinstance(other,ColX) and all(getattr(self,a)==getattr(other,a) for a in self.ATTRS)
class PKeyX(BaseX):
  def __init__(self,fields): self.fields=fields
  def __repr__(self): return 'PKeyX(%r)'%(self.fields,)
  def __eq__(self,other): return isinstance(other,PKeyX) and self.fields==other.fields
class CreateX(CommandX):
  def __init__(self,nexists,name,cols,pkey): self.nexists,self.name,self.cols,self.pkey=nexists,name,cols,pkey
  def __repr__(self): return 'CreateX(%r,%r,%r,%s)'%(self.nexists,self.name,self.cols,self.pkey)
  def __eq__(self,other): return isinstance(other,CreateX) and (self.nexists,self.name,self.cols,self.pkey)==(other.nexists,other.name,other.cols,other.pkey)

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

class DeleteX(CommandX):
  def __init__(self,table,where): self.table,self.where=table,where
  def __repr__(self): return 'DeleteX(%r,%r)'%(self.table,self.where)
  def __eq__(self,other): return isinstance(other,DeleteX) and (self.table,self.where)==(other.table,other.where)

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

def ulkw(kw): "uppercase/lowercase keyword"; return '%s|%s'%(kw.lower(),kw.upper())

KEYWORDS = {w:'kw_'+w for w in 'array case when then else end as join on from where order by limit offset select is not and or in null default primary key if exists create table'.split()}
class SqlGrammar:
  # todo: adhere more closely to the spec. http://www.postgresql.org/docs/9.1/static/sql-syntax-lexical.html
  t_STRLIT = "'((?<=\\\\)'|[^'])+'"
  t_INTLIT = '\d+'
  t_SUBLIT = '%s'
  t_ARITH = '\|\||\/|\+'
  t_CMP = '\!=|@>|<|>'
  def t_NAME(self,t):
    '[A-Za-z]\w*'
    # warning: this allows stuff like SeLeCt with mixed case. who cares.
    t.type = KEYWORDS[t.value.lower()] if t.value.lower() in KEYWORDS else 'BOOL' if t.value.lower() in ('is','not') else 'NAME'
    return t
  literals = ('[',']','(',')','{','}',',','.','*','=','-')
  t_ignore = ' '
  def t_error(self,t): raise SQLSyntaxError(t) # I think t is LexToken(error,unparsed_tail)
  tokens = (
    # general
    'STRLIT','INTLIT','NAME','SUBLIT',
    # operators
    'ARITH','CMP',
  ) + tuple(KEYWORDS.values())
  precedence = (
    # ('left','DOT'),
  )
  def p_name(self,t): "expression : NAME"; t[0] = NameX(t[1])
  def p_float(self,t): "expression : INTLIT '.' INTLIT"; t[0] = Literal(float('%s.%s'%(t[1],t[3])))
  def p_int(self,t): "expression : INTLIT"; t[0] = Literal(int(t[1]))
  def p_strlit(self,t): "expression : STRLIT"; t[0] = Literal(t[1][1:-1])
  def p_asterx(self,t): "expression : '*'"; t[0] = AsterX()
  def p_null(self,t): "expression : kw_null"; t[0] = NullX()
  def p_unop(self,t): "unop : '-' \n | kw_not"; t[0] = OpX(t[1])
  def p_isnot(self,t): "isnot : kw_is kw_not"; t[0] = 'is not'
  def p_boolop(self,t): "boolop : kw_and \n | kw_or \n | kw_in"; t[0] = t[1]
  def p_binop(self,t):
    "binop : ARITH \n | CMP \n | boolop \n | isnot \n | '=' \n | '-'"
    t[0] = OpX(t[1])
  def p_x_boolx(self,t):
    """expression : unop expression
                  | expression binop expression
    """
    # todo: ply exposes precedence with %prec, use it.
    if len(t)==4: t[0] = bin_priority(t[2],t[1],t[3])
    elif len(t)==3: t[0] = un_priority(t[1],t[2])
    else: raise NotImplementedError('unk_len',len(t))
  def p_x_commalist(self,t):
    """commalist : commalist ',' expression
                 | expression
    """
    if len(t) == 2: t[0] = CommaX([t[1]])
    elif len(t) == 4: t[0] = CommaX(t[1].children+[t[3]])
    else: raise NotImplementedError('unk_len',len(t))
  def p_array(self,t):
    """expression : '{' commalist '}'
                  | kw_array '[' commalist ']'
    """
    if len(t)==4: t[0] = ArrayLit(t[2].children)
    elif len(t)==5: t[0] = ArrayLit(t[3].children)
    else: raise NotImplementedError('unk_len',len(t))
  def p_whenlist(self,t):
    """whenlist : whenlist kw_when expression kw_then expression
                | kw_when expression kw_then expression
    """
    if len(t)==5: t[0] = [WhenX(t[2],t[4])]
    elif len(t)==6: t[0] = t[1] + [WhenX(t[3],t[5])]
    else: raise NotImplementedError('unk_len',len(t))
  def p_case(self,t):
    """expression : kw_case whenlist kw_else expression kw_end
                  | kw_case whenlist kw_end
    """
    if len(t)==4: t[0] = CaseX(t[2],None)
    elif len(t)==6: t[0] = CaseX(t[2],t[4])
    else: raise NotImplementedError('unk_len',len(t))
  def p_call(self,t):
    "expression : NAME '(' commalist ')'"
    t[0] = CallX(NameX(t[1]), t[3].children)
  def p_attr(self,t):
    """expression : NAME '.' NAME
                  | NAME '.' '*'
    """
    t[0] = AttrX(NameX(t[1]), AsterX() if t[3]=='*' else NameX(t[3]))
  def p_paren(self,t):
    "expression : '(' expression ')' \n | '(' commalist ')'" # todo doc: think about this
    t[0] = t[2]
  def p_fromtable(self,t):
    """fromtable : NAME
                 | NAME kw_as NAME
    """
    # elif node is clas.from_table: return FromTableX(x[1].name, None if len(x)==2 else x[3].name)
    if len(t) not in (2,4): raise NotImplementedError('unk_len',len(t))
    t[0] = FromTableX(t[1],t[3] if len(t) == 4 else None)
  def p_joinx(self,t):
    """joinx : fromtable kw_join fromtable
             | fromtable kw_join fromtable kw_on expression
    """
    raise NotImplementedError('joinx')
  def p_fromitem(self,t): "fromitem : fromtable \n | joinx"; t[0] = t[1]
  def p_fromitem_list(self,t):
    """fromitem_list : fromitem_list ',' fromitem
                     | fromitem
    """
    if len(t)==2: t[0] = [t[1]]
    elif len(t)==4: t[0] = t[1] + [t[3]]
    else: raise NotImplementedError('unk_len', len(t))
  def p_fromlist(self,t): "fromlist : kw_from fromitem_list \n | "; t[0] = FromListX(t[2] if len(t) == 3 else [])
  def p_wherex(self,t): "wherex : kw_where expression \n | "; t[0] = t[2] if len(t) == 3 else None
  def p_order(self,t): "order : kw_order kw_by expression \n | "; t[0] = t[3] if len(t) == 4 else None
  def p_limit(self,t): "limit : kw_limit expression \n | "; t[0] = t[2] if len(t) == 3 else None
  def p_offset(self,t): "offset : kw_offset expression \n | "; t[0] = t[2] if len(t) == 3 else None
  def p_selectx(self,t):
    "expression : kw_select commalist fromlist wherex order limit offset"
    t[0] = SelectX(*t[2:])
  def p_isarray(self,t): "is_array : '[' ']' \n | "; t[0] = len(t) > 1
  def p_isnotnull(self,t): "is_notnull : kw_not kw_null \n | "; t[0] = len(t) > 1
  def p_default(self,t): "default : kw_default expression \n | "; t[0] = t[2] if len(t) > 1 else None
  def p_ispkey(self,t): "is_pkey : kw_primary kw_key \n | "; t[0] = len(t) > 1
  def p_colspec(self,t):
    "col_spec : NAME NAME is_array is_notnull default is_pkey"
    t[0] = ColX(*t[1:])
  def p_createlist(self,t):
    "create_list : create_list ',' col_spec \n | col_spec"
    if len(t)==2: t[0] = [t[1]]
    elif len(t)==4: t[0] = t[1] + [t[3]]
    else: raise NotImplementedError('unk_len',len(t))
  def p_namelist(self,t):
    "namelist : namelist ',' NAME \n | NAME"
    if len(t)==2: t[0] = [t[1]]
    elif len(t)==4: t[0] = t[1] + [t[3]]
    else: raise NotImplementedError('unk_len',len(t))
  def p_pkey(self,t): "pkey_stmt : ',' kw_primary kw_key '(' namelist ')' \n | "; t[0] = PKeyX(t[5]) if len(t) > 1 else None
  def p_nexists(self,t): "nexists : kw_if kw_not kw_exists \n | "; t[0] = len(t) > 1
  def p_createx(self,t):
    "expression : kw_create kw_table nexists NAME '(' create_list pkey_stmt ')'"
    t[0] = CreateX(t[3],t[4],t[6],t[7])

  def p_error(self,t):
    print 'error',t
    raise NotImplementedError

LEXER = ply.lex.lex(module=SqlGrammar())
def lex(string):
  "this is only used by tests"
  safe_lexer = LEXER.clone() # reentrant? I can't tell, I hate implicit globals. do a threading test
  safe_lexer.input(string)
  a = []
  while 1:
    t = safe_lexer.token()
    if t: a.append(t)
    else: break
  return a

YACC = ply.yacc.yacc(module=SqlGrammar(),debug=0,write_tables=0)
def parse(string):
  "return a BaseX tree for the string"
  return YACC.parse(string, lexer=LEXER.clone())
