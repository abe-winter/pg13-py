"parsing for pgmock -- rewritten in PLY"

# todo: be more consistent about CommaX vs raw tuple
# todo: factor out the AST-path stuff to its own file

# differences vs real SQL:
# 1. sql probably allows 'table' as a table name. I think I'm stricter about keywords (and I don't allow quoting columns)

import ply.lex, ply.yacc, itertools
from . import treepath

# errors
class PgMockError(StandardError): pass
class SQLSyntaxError(PgMockError):
  """base class for errors during parsing.
  beware: this gets called during table execution for things we should catch during parsing.
  """

class BaseX(treepath.PathTree):
  "base class for expressions"
  ATTRS=()
  VARLEN=()
  def __init__(self,*args):
    if len(args)!=len(self.ATTRS): raise TypeError('wrong_n_args',len(args),len(self.ATTRS),args)
    for attr,arg in zip(self.ATTRS,args): setattr(self,attr,arg)
  def __eq__(self,other):
    return type(self) is type(other) and all(getattr(self,attr)==getattr(other,attr) for attr in self.ATTRS)
  def __repr__(self):
    return '%s(%s)'%(self.__class__.__name__,','.join(map(repr,(getattr(self,attr) for attr in self.ATTRS))))

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
class AliasX(BaseX): ATTRS = ('name','alias')

class JoinTypeX(BaseX):
  ATTRS = ('side','outer','natural')
  @property
  def inner(self): return not self.is_outer

class JoinX(BaseX): ATTRS = ('a','b','on_stmt','jointype')
class OpX(BaseX):
  PRIORITY=('or','and','not','>','<','@>','@@','||','!=','=','is not','is','in','*','/','+','-')
  ATTRS = ('op',)
  def __init__(self,op):
    self.op=op
    if op not in self.PRIORITY: raise SQLSyntaxError('unk_op',op)
  def __lt__(self,other):
    "this is for order of operations"
    if not isinstance(other,OpX): raise TypeError
    return self.PRIORITY.index(self.op) < self.PRIORITY.index(other.op)
class BinX(BaseX): ATTRS = ('op','left','right')
class UnX(BaseX): ATTRS = ('op','val')
class CommaX(BaseX):
  # todo: implement an __iter__ for this; I
  ATTRS = ('children',)
  VARLEN = ('children',)
  def __init__(self,children): self.children=list(children) # needs to be a list (i.e. mutable) for SubLit substitution (in sqex.sub_slots)
  def __iter__(self): raise NotImplementedError("don't iterate CommaX directly -- loop on x.children")
class CallX(BaseX): ATTRS = ('f','args') # args is not VARLEN; it's a commax because it's passed to evalex I think
class WhenX(BaseX): ATTRS = ('when','then')
class CaseX(BaseX):
  ATTRS = ('cases','elsex')
  VARLEN = ('cases',)
class AttrX(BaseX): ATTRS = ('parent','attr')
class TypeX(BaseX): ATTRS = ('type','width') # width is e.g. 20 for VARCHAR(20), null for e.g. TEXT.
class CastX(BaseX): ATTRS = ('expr','to_type')

class CommandX(BaseX): "base class for top-level commands. probably won't ever be used."
class SelectX(CommandX):
  ATTRS = ('cols','tables','where','group','order','limit','offset')
  VARLEN = ('tables',)

class ColX(BaseX): ATTRS = ('name','coltp','isarray','not_null','default','pkey')
class PKeyX(BaseX):
  ATTRS = ('fields',)
  VARLEN = ('fields',)
class TableConstraintX(BaseX): "intermediate base class for table constraints. PKeyX isn't included in this because it has its own slot in CreateX."
class CheckX(TableConstraintX): ATTRS = ('expr',)
class CreateX(CommandX):
  "note: technically pkey is a table_constraint but it also comes from cols so its separate"
  ATTRS = ('nexists','name','cols','pkey','table_constraints','inherits')
  VARLEN = ('cols','table_constraints') # todo: is pkey varlen or CommaX?
class DropX(CommandX): ATTRS = ('ifexists','name','cascade')

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

class StartX(CommandX): ATTRS = ()
class CommitX(CommandX): ATTRS = ()
class RollbackX(CommandX): ATTRS = ()

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

KEYWORDS = {w:'kw_'+w for w in 'array case when then else end as join on from where order by limit offset select is not and or in null default primary key if exists create table insert into values returning update set delete group inherits check constraint start transaction commit rollback left right full inner outer using drop cascade cast'.split()}
class SqlGrammar:
  # todo: adhere more closely to the spec. http://www.postgresql.org/docs/9.1/static/sql-syntax-lexical.html
  t_STRLIT = "'((?<=\\\\)'|[^'])+'"
  t_INTLIT = '\d+'
  t_SUBLIT = '%s'
  t_ARITH = '\|\||\/|\+'
  t_CMP = '\!=|@>|@@|<|>'
  t_CAST = '::'
  def t_NAME(self,t):
    '[A-Za-z]\w*|\"char\"'
    # warning: this allows stuff like SeLeCt with mixed case. who cares.
    t.type = KEYWORDS[t.value.lower()] if t.value.lower() in KEYWORDS else 'BOOL' if t.value.lower() in ('is','not') else 'NAME'
    return t
  literals = ('[',']','(',')','{','}',',','.','*','=','-')
  t_ignore = ' \n\t'
  def t_error(self,t): raise SQLSyntaxError(t) # I think t is LexToken(error,unparsed_tail)
  tokens = (
    # general
    'STRLIT','INTLIT','NAME','SUBLIT',
    # operators
    'ARITH','CMP','CAST',
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
  def p_typename(self,t):
    "typename : NAME \n | NAME '(' INTLIT ')'"
    t[0] = TypeX(t[1],None) if len(t) == 2 else TypeX(t[1],int(t[3]))
  def p_castx(self,t): "expression : expression CAST typename"; t[0] = CastX(t[1],t[3])
  def p_castx2(self,t):
    "expression : kw_cast '(' expression kw_as typename ')'"
    # the second expression should be some kind of type spec. use it in createx and 'x cast y' also
    t[0] = CastX(t[3],t[5])
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
    else: raise NotImplementedError('unk_len',len(t)) # pragma: no cover
  def p_x_commalist(self,t):
    """commalist : commalist ',' expression
                 | expression
    """
    if len(t) == 2: t[0] = CommaX([t[1]])
    elif len(t) == 4: t[0] = CommaX(t[1].children+[t[3]])
    else: raise NotImplementedError('unk_len',len(t)) # pragma: no cover
  def p_array(self,t):
    """expression : '{' commalist '}'
                  | kw_array '[' commalist ']'
    """
    if len(t)==4: t[0] = ArrayLit(t[2].children)
    elif len(t)==5: t[0] = ArrayLit(t[3].children)
    else: raise NotImplementedError('unk_len',len(t)) # pragma: no cover
  def p_whenlist(self,t):
    """whenlist : whenlist kw_when expression kw_then expression
                | kw_when expression kw_then expression
    """
    if len(t)==5: t[0] = [WhenX(t[2],t[4])]
    elif len(t)==6: t[0] = t[1] + [WhenX(t[3],t[5])]
    else: raise NotImplementedError('unk_len',len(t)) # pragma: no cover
  def p_case(self,t):
    """expression : kw_case whenlist kw_else expression kw_end
                  | kw_case whenlist kw_end
    """
    if len(t)==4: t[0] = CaseX(t[2],None)
    elif len(t)==6: t[0] = CaseX(t[2],t[4])
    else: raise NotImplementedError('unk_len',len(t)) # pragma: no cover
  def p_call(self,t):
    "expression : NAME '(' commalist ')'"
    t[0] = CallX(t[1], t[3])
  def p_attr(self,t):
    """attr : NAME '.' NAME
            | NAME '.' '*'
    """
    # careful: sqex.infer_columns relies on AttrX not containing anything but a name
    t[0] = AttrX(NameX(t[1]), AsterX() if t[3]=='*' else NameX(t[3]))
  def p_attrx(self,t): "expression : attr"; t[0] = t[1]
  def p_aliasx(self,t): "aliasx : expression kw_as NAME"; t[0] = AliasX(t[1],t[3])
  def p_paren(self,t):
    "expression : '(' expression ')' \n | '(' commalist ')'" # todo doc: think about this
    t[0] = t[2]
  def p_fromtable(self,t):
    """fromtable : NAME
                 | aliasx
                 | '(' selectx ')' kw_as NAME
    """
    if len(t)==6: t[0]=AliasX(t[2],t[5])
    elif len(t)==2: t[0]=t[1]
    else: raise NotImplementedError('unk_len',len(t)) # pragma: no cover
  def p_outerjoin(self,t): "outerjoin : kw_left \n | kw_right \n | kw_full"; t[0] = t[1]
  def p_jointype(self,t):
    """jointype : kw_join
                | kw_inner kw_join
                | outerjoin kw_outer kw_join
                | outerjoin kw_join
    """
    if len(t) <= 2 or t[1] == 'inner': t[0] = JoinTypeX(None, False, None)
    else: t[0] = JoinTypeX(t[1], True, None)
  def p_joinx(self,t):
    # todo: support join types http://www.postgresql.org/docs/9.4/static/queries-table-expressions.html#QUERIES-JOIN
    """joinx : fromtable jointype fromtable
             | fromtable jointype fromtable kw_on expression
             | fromtable jointype fromtable kw_using '(' namelist ')'
    """
    if len(t)==4: t[0] = JoinX(t[1],t[3],None,t[2])
    elif len(t)==6: t[0] = JoinX(t[1],t[3],t[5],t[2])
    else: raise NotImplementedError('todo: join .. using')
  def p_fromitem(self,t): "fromitem : fromtable \n | joinx"; t[0] = t[1]
  def p_fromitem_list(self,t):
    """fromitem_list : fromitem_list ',' fromitem
                     | fromitem
    """
    if len(t)==2: t[0] = [t[1]]
    elif len(t)==4: t[0] = t[1] + [t[3]]
    else: raise NotImplementedError('unk_len', len(t)) # pragma: no cover
  def p_fromlist(self,t): "fromlist : kw_from fromitem_list \n | "; t[0] = t[2] if len(t) == 3 else []
  def p_wherex(self,t): "wherex : kw_where expression \n | "; t[0] = t[2] if len(t) == 3 else None
  def p_order(self,t): "order : kw_order kw_by expression \n | "; t[0] = t[3] if len(t) == 4 else None
  def p_limit(self,t): "limit : kw_limit expression \n | "; t[0] = t[2] if len(t) == 3 else None
  def p_offset(self,t): "offset : kw_offset expression \n | "; t[0] = t[2] if len(t) == 3 else None
  def p_group(self,t): "group : kw_group kw_by expression \n | "; t[0] = t[3] if len(t)==4 else None
  def p_selectx(self,t):
    "selectx : kw_select commalist fromlist wherex group order limit offset"
    t[0] = SelectX(*t[2:])
  def p_extra_x(self,t): "expression : selectx \n | aliasx"; t[0] = t[1] # expressions that also need to be separately addressable
  def p_isarray(self,t): "is_array : '[' ']' \n | "; t[0] = len(t) > 1
  def p_isnotnull(self,t): "is_notnull : kw_not kw_null \n | "; t[0] = len(t) > 1
  def p_default(self,t): "default : kw_default expression \n | "; t[0] = t[2] if len(t) > 1 else None
  def p_ispkey(self,t): "is_pkey : kw_primary kw_key \n | "; t[0] = len(t) > 1
  def p_colspec(self,t):
    "col_spec : NAME typename is_array is_notnull default is_pkey"
    # todo: integrate is_array into typename
    t[0] = ColX(*t[1:])
  def p_namelist(self,t):
    "namelist : namelist ',' NAME \n | NAME"
    if len(t)==2: t[0] = [t[1]]
    elif len(t)==4: t[0] = t[1] + [t[3]]
    else: raise NotImplementedError('unk_len',len(t)) # pragma: no cover
  def p_pkey(self,t): "pkey_stmt : kw_primary kw_key '(' namelist ')'"; t[0] = PKeyX(t[4])
  def p_nexists(self,t): "nexists : kw_if kw_not kw_exists \n | "; t[0] = len(t) > 1
  def p_opt_inheritx(self,t):
    "opt_inheritx : inheritx \n | "
    t[0] = None if len(t)==1 else t[1]
  def p_inheritx(self,t):
    "inheritx : kw_inherits '(' namelist ')'"
    t[0] = t[3]
  def p_constraint_name(self,t):
    "opt_constraint_name : kw_constraint NAME \n | "
    t[0] = None if len(t) == 1 else t[2]
  def p_tconstraint_check(self,t):
    "table_constraint : opt_constraint_name kw_check '(' expression ')'"
    t[0] = CheckX(t[4])
  def p_tablespec(self,t):
    "tablespec : col_spec \n | pkey_stmt \n | table_constraint"
    t[0] = t[1]
  def p_tablespecs(self,t):
    "tablespecs : tablespecs ',' tablespec \n | tablespec"
    t[0] = [t[1]] if len(t)==2 else t[1] + [t[3]]
  def p_createx(self,t):
    """expression : kw_create kw_table nexists NAME '(' tablespecs ')' opt_inheritx
                  | kw_create kw_table nexists NAME inheritx
    """
    if len(t)==6:
      t[0] = CreateX(t[3], t[4], [], None, [], t[5])
    else:
      all_constraints = {k:list(group) for k, group in itertools.groupby(t[6], lambda x:type(x))}
      pkey = (all_constraints.get(PKeyX) or [None])[0] # todo: get pkey from column constraints as well
      if PKeyX in all_constraints and len(all_constraints[PKeyX]) != 1:
        raise SQLSyntaxError('too_many_pkeyx', all_constraints[PKeyX])
      # note below: this is a rare case where issubclass is safe
      table_constraints = sum([v for k,v in all_constraints.items() if issubclass(k, TableConstraintX)], [])
      t[0] = CreateX(t[3], t[4], all_constraints.get(ColX) or [], pkey, table_constraints, t[8])
  def p_ifexists(self,t): "ifexists : kw_if kw_exists \n | "; t[0] = len(t) > 1
  def p_cascade(self,t): "cascade : kw_cascade \n | "; t[0] = len(t) > 1
  def p_dropx(self,t):
    "expression : kw_drop kw_table ifexists NAME cascade"
    t[0] = DropX(t[3],t[4],t[5])
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
    else: raise NotImplementedError('unk_len', len(t)) # pragma: no cover
  def p_updatex(self,t):
    "expression : kw_update namelist kw_set assignlist wherex opt_returnx"
    t[0] = UpdateX(t[2],t[4],t[5],t[6])
  def p_deletex(self,t):
    "expression : kw_delete kw_from NAME wherex opt_returnx"
    t[0] = DeleteX(t[3],t[4],t[5])

  # todo: these aren't really expressions; they can only be used at top-level. sqex will catch it. should the syntax know?
  def p_startx(self,t): "expression : kw_start kw_transaction"; t[0] = StartX()
  def p_commitx(self,t): "expression : kw_commit"; t[0] = CommitX()
  def p_rollbackx(self,t): "expression : kw_rollback"; t[0] = RollbackX()

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
