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
  ATTRS=()
  VARLEN=()
  def __init__(self,*args):
    if len(args)!=len(self.ATTRS): raise TypeError('wrong_n_args',len(args),len(self.ATTRS))
    for attr,arg in zip(self.ATTRS,args): setattr(self,attr,arg)
  def __eq__(self,other):
    return type(self) is type(other) and all(getattr(self,attr)==getattr(other,attr) for attr in self.ATTRS)
  def __repr__(self):
    return '%s(%s)'%(self.__class__.__name__,','.join(map(repr,(getattr(self,attr) for attr in self.ATTRS))))
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
  ATTRS = ('val',)
  def toliteral(self): return self.val # is this still used?
class ArrayLit(BaseX): # todo: this isn't always a literal. what if there's a select stmt inside? yikes.
  ATTRS = ('vals',)
  VARLEN = ('vals',)
  def __init__(self,vals): self.vals = list(vals)
  def toliteral(self): return self.vals # todo: get rid?
class SubLit(object): pass

class NameX(BaseX): ATTRS = ('name',)
class AsterX(BaseX): pass
class NullX(BaseX): pass
class FromTableX(BaseX): ATTRS = ('name','alias')
class JoinX(BaseX): ATTRS = ('a','b','on_stmt')
class OpX(BaseX):
  PRIORITY=('or','and','not','>','<','@>','||','!=','=','is not','is','in','*','/','+','-') # not is tight because it's unary when solo
  ATTRS = ('op',)
  def __init__(self,op):
    self.op=op
    if op not in self.PRIORITY: raise SQLSyntaxError('unk_op',op)
  def __lt__(self,other):
    "this is for order of operations"
    if not isinstance(other,OpX): raise TypeError
    return self.PRIORITY.index(self.op) < self.PRIORITY.index(other.op)
class BinX(BaseX):
  "binary operator expression"
  ATTRS = ('op','left','right')
class UnX(BaseX):
  "unary operator expression"
  ATTRS = ('op','val')
class CommaX(BaseX):
  ATTRS = ('children',)
  VARLEN = ('children',)
  def __init__(self,children): self.children=list(children) # needs to be a list (i.e. mutable) for SubLit substitution (in sqex.sub_slots)
class CallX(BaseX): ATTRS = ('f','args') # args is not VARLEN; it's a commax because it's passed to evalex I think
class WhenX(BaseX): ATTRS = ('when','then')
class CaseX(BaseX):
  ATTRS = ('cases','elsex')
  VARLEN = ('cases',)
class AttrX(BaseX): ATTRS = ('parent','attr')

class CommandX(BaseX): "base class for top-level commands. probably won't ever be used."
class SelectX(CommandX):
  ATTRS = ('cols','tables','where','order','limit','offset')
  VARLEN = ('tables',)

class ColX(BaseX): ATTRS = ('name','coltp','isarray','not_null','default','pkey')
class PKeyX(BaseX):
  ATTRS = ('fields',)
  VARLEN = ('fields',)
class CreateX(CommandX):
  ATTRS = ('nexists','name','cols','pkey')
  VARLEN = ('cols',)

class ReturnX(BaseX): ATTRS = ('expr',)
class InsertX(CommandX):
  ATTRS = ('table','cols','values','ret')
  VARLEN = ('cols','values')

class AssignX(BaseX): ATTRS = ('col','expr')
class UpdateX(CommandX):
  ATTRS = ('tables','assigns','where','ret')
  VARLEN = ('tables','assigns')

class IndexX(CommandX): ATTRS = ('string',)

class DeleteX(CommandX): ATTRS = ('table','where','returnx')

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

KEYWORDS = {w:'kw_'+w for w in 'array case when then else end as join on from where order by limit offset select is not and or in null default primary key if exists create table insert into values returning update set delete'.split()}
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
  def p_strlit(self,t): "expression : STRLIT"; t[0] = Literal(t[1][1:-1].replace("\\'","'")) # warning: this is not safe
  def p_asterx(self,t): "expression : '*'"; t[0] = AsterX()
  def p_null(self,t): "expression : kw_null"; t[0] = NullX()
  def p_sublit(self,t): "expression : SUBLIT"; t[0] = SubLit
  def p_unop(self,t): "unop : '-' \n | kw_not"; t[0] = OpX(t[1])
  def p_isnot(self,t): "isnot : kw_is kw_not"; t[0] = 'is not'
  def p_boolop(self,t): "boolop : kw_and \n | kw_or \n | kw_in"; t[0] = t[1]
  def p_binop(self,t):
    "binop : ARITH \n | CMP \n | boolop \n | isnot \n | '=' \n | '-' \n | '*' \n | kw_is"
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
    t[0] = CallX(t[1], t[3])
  def p_attr(self,t):
    """attr : NAME '.' NAME
                  | NAME '.' '*'
    """
    t[0] = AttrX(NameX(t[1]), AsterX() if t[3]=='*' else NameX(t[3]))
  def p_attrx(self,t): "expression : attr"; t[0] = t[1]
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
    if len(t)==4: t[0] = JoinX(t[1],t[3],None)
    elif len(t)==6: t[0] = JoinX(t[1],t[3],t[5])
    else: raise NotImplementedError('unk_len',len(t))
  def p_fromitem(self,t): "fromitem : fromtable \n | joinx"; t[0] = t[1]
  def p_fromitem_list(self,t):
    """fromitem_list : fromitem_list ',' fromitem
                     | fromitem
    """
    if len(t)==2: t[0] = [t[1]]
    elif len(t)==4: t[0] = t[1] + [t[3]]
    else: raise NotImplementedError('unk_len', len(t))
  def p_fromlist(self,t): "fromlist : kw_from fromitem_list \n | "; t[0] = t[2] if len(t) == 3 else []
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
  def p_colspec(self,t): "col_spec : NAME NAME is_array is_notnull default is_pkey"; t[0] = ColX(*t[1:])
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
  def p_returnx(self,t):
    "opt_returnx : kw_returning commalist \n | "
    # note: this gets weird because '(' commalist ')' is an expression but we need bare commalist to support non-paren returns
    t[0] = None if len(t)==1 else ReturnX(t[2].children[0] if len(t[2].children)==1 else t[2])
  def p_optparennamelist(self,t): "opt_paren_namelist : '(' namelist ')' \n | "; t[0] = t[2] if len(t)>1 else None
  def p_insertx(self,t):
    "expression : kw_insert kw_into NAME opt_paren_namelist kw_values '(' commalist ')' opt_returnx"
    t[0] = InsertX(t[3],t[4],t[7].children,t[9])
  def p_assign(self,t):
    "assign : NAME '=' expression \n | attr '=' expression"
    t[0] = AssignX(t[1],t[3])
  def p_assignlist(self,t):
    "assignlist : assignlist ',' assign \n | assign"
    if len(t)==4: t[0] = t[1] + [t[3]]
    elif len(t)==2: t[0] = [t[1]]
    else: raise NotImplementedError('unk_len', len(t))
  def p_updatex(self,t):
    "expression : kw_update namelist kw_set assignlist wherex opt_returnx"
    t[0] = UpdateX(t[2],t[4],t[5],t[6])
  def p_deletex(self,t):
    "expression : kw_delete kw_from NAME wherex opt_returnx"
    t[0] = DeleteX(t[3],t[4],t[5])

  def p_error(self,t): raise SQLSyntaxError(t)

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
  print string
  if string.strip().lower().startswith('create index'): return IndexX(string)
  return YACC.parse(string, lexer=LEXER.clone())
