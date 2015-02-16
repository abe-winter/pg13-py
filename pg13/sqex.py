"expression evaluation helpers for pgmock. has duck-dependencies on pgmock's Table class, needs redesign."

import functools,itertools
from . import sqparse2,threevl

# todo: derive errors below from something pg13-specific
class ColumnNameError(StandardError): "name not in any tables or name matches too many tables"
class TableNameError(StandardError): "expression referencing unk table"

def is_aggregate(ex):
  "is the expression something that runs on a list of rows rather than a single row"
  # todo: I think the SQL term for this is 'scalar subquery'. use SQL vocabulary
  # todo doc: explain why it's not necessary to do this check on the whereclause
  return isinstance(ex,sqparse2.CallX) and ex.f in ('min','max')
def contains_aggregate(ex): return bool(sub_slots(ex,is_aggregate))

def evalop(op,left,right):
  "this takes evaluated left and right (i.e. values not expressions)"
  if op in ('=','!=','>','<'): return threevl.ThreeVL.compare(op,left,right)
  elif op in ('+','-','*','/'): # todo: does arithmetic require threevl?
    if op=='/': raise NotImplementedError('todo: spec about int/float division')
    return (left + right) if op=='+' else (left - right) if op=='-' else (left * right) if op=='*' else (left/right)
  elif op=='in': return (tuple(left) in right) if isinstance(left,list) and isinstance(right[0],tuple) else (left in right)
  elif op in ('and','or'): return threevl.ThreeVL.andor(op,left,right)
  elif op in ('is not','is'):
    if right is not None: raise NotImplementedError('can null be on either side? what if neither value is null?')
    return (left is not None) if (op=='is not') else (left is None)
  elif op=='@>':
    if not all(isinstance(x,list) for x in (left,right)): raise TypeError('non-array-args',self.value)
    return set(left)>=set(right)
  elif op=='||':
    if not all(isinstance(x,list) for x in (left,right)): raise TypeError('non-array-args',self.value)
    return left+right
  else: raise NotImplementedError(op,left,right)

class NameIndexer:
  "helper that takes str, NameX or attrx and returns the right thing (or raises an error on ambiguity)"
  @staticmethod
  def update_aliases(aliases,ftx):
    "helper for ctor. takes FromTableX"
    aliases[ftx.name]=ftx.name
    if ftx.alias: aliases[ftx.alias]=ftx.name
  @classmethod
  def ctor_fromlist(clas,fromlistx):
    aliases={}
    for from_item in fromlistx:
      if isinstance(from_item,sqparse2.FromTableX): clas.update_aliases(aliases,from_item)
      elif isinstance(from_item,sqparse2.JoinX):
        clas.update_aliases(aliases,from_item.a)
        clas.update_aliases(aliases,from_item.b)
      else: raise TypeError(type(from_item))
    table_order=sorted(set(aliases.values()))
    return clas(aliases,table_order)
  @classmethod
  def ctor_name(clas,name): return clas({name:name},[name])
  def __init__(self,aliases,table_order): self.aliases,self.table_order = aliases,table_order
  def index_tuple(self,tables_dict,index,is_set):
    "helper for rowget/rowset"
    if isinstance(index,sqparse2.NameX): index = index.name
    # careful below: intentionally if, not elif
    if isinstance(index,basestring):
      candidates = [t for t in self.table_order if any(f.name==index for f in tables_dict[t].fields)]
      if len(candidates)!=1: raise ColumnNameError(("ambiguous_column" if candidates else "no_such_column"),index)
      tname, = candidates
      return self.table_order.index(tname), tables_dict[tname].lookup(index).index
    if isinstance(index,sqparse2.AttrX):
      if index.parent.name not in self.aliases: raise TableNameError('table_notin_x',index.parent)
      tname = self.aliases[index.parent.name]
      tindex = self.table_order.index(tname)
      if isinstance(index.attr,sqparse2.AsterX):
        if is_set: raise ValueError('cant_set_asterisk') # todo: better error class
        else: return (tindex,)
      else: return (tindex,tables_dict[tname].lookup(index.attr).index)
      # todo: stronger typing here. make sure both fields of the AttrX are always strings.
    else: raise TypeError(type(index))
  def rowget(self,tables_dict,row_list,index):
    "row_list in self.row_order"
    # print 'rowget', self.index_tuple(tables_dict,index,False), row_list
    tmp=row_list
    for i in self.index_tuple(tables_dict,index,False): tmp=tmp[i]
    return tmp
  def rowset(self,tables_dict,row_list,index): raise NotImplementedError # note: shouldn't need this until update uses this
  def __repr__(self): return '<NameIndexer %s>'%self.table_order

def decompose_select(selectx):
  "return [(parent,setter) for scalar_subquery], wherex_including_on, NameIndexer. helper for run_select"
  nix = NameIndexer.ctor_fromlist(selectx.tables)
  where = []
  for fromx in selectx.tables:
    if isinstance(fromx,sqparse2.JoinX) and fromx.on_stmt is not None:
      # todo: what happens if on_stmt columns are non-ambiguous in the context of the join tables but ambiguous overall? yuck.
      where.append(fromx.on_stmt)
  if selectx.where: where.append(selectx.where)
  return nix, where

def eval_where(where_list,composite_row,nix,tables_dict):
  "join-friendly whereclause evaluator. composite_row is a list or tuple of row lists. where_list is the thing from decompose_select."
  # todo: do I need to use 3vl instead of all() to merge where_list?
  return all(evalex(w,composite_row,nix,tables_dict) for w in where_list)

def flatten_scalar(whatever):
  "warning: there's a systematic way to do this and I'm doing it blindly. In particular, this will screw up arrays."
  try: flat1=whatever[0]
  except IndexError: return None
  try: return flat1[0]
  except TypeError: return flat1

def replace_subqueries(ex,tables):
  # http://www.postgresql.org/docs/9.1/static/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES
  # see here for subquery conditions that *do* use multi-rows. ug. http://www.postgresql.org/docs/9.1/static/functions-subquery.html
  for path in sub_slots(ex, lambda x:isinstance(x,sqparse2.SelectX)):
    ex[path] = sqparse2.Literal(flatten_scalar(run_select(ex[path], tables)))
  return ex # but it was modified in place, too

def run_select(ex,tables):
  nix, where = decompose_select(ex)
  composite_rows = [c_row for c_row in itertools.product(*(tables[t].rows for t in nix.table_order)) if eval_where(where,c_row,nix,tables)]
  if ex.order: # note: order comes before limit / offset
    composite_rows.sort(key=lambda c_row:evalex(ex.order,c_row,nix,tables))
  if ex.limit or ex.offset:
    print ex.limit, ex.offset
    raise NotImplementedError('notimp: limit,offset')
  if contains_aggregate(ex.cols):
    if not all(is_aggregate(col) or contains_aggregate(col) for col in ex.cols.children):
      # todo: this isn't good enough. what about nesting cases like max(min(whatever))
      raise sqparse2.SQLSyntaxError('not_all_aggregate') # is this the way real PG works? aim at giving PG error codes
    return evalex(ex.cols,composite_rows,nix,tables)
  else: return [evalex(ex.cols,r,nix,tables) for r in composite_rows]

def starlike(x):
  "weird things happen to cardinality when working with * in comma-lists. this detects when to do that."
  return isinstance(x,sqparse2.AsterX) or isinstance(x,sqparse2.AttrX) and isinstance(x.attr,sqparse2.AsterX)

def evalex(x,c_row,nix,tables):
  "c_row is a composite row, i.e. a list/tuple of rows from all the query's tables, ordered by nix.table_order"
  def subcall(x): "helper for recursive calls"; return evalex(x,c_row,nix,tables)
  if not isinstance(nix, NameIndexer): raise NotImplementedError # todo: remove this once Table meths are all up-to-date
  if isinstance(x,sqparse2.BinX):
    l,r=map(subcall,(x.left,x.right))
    return evalop(x.op.op,l,r)
  elif isinstance(x,sqparse2.UnX):
    inner=subcall(x.val)
    if x.op.op=='+': return inner
    elif x.op.op=='-': return -inner
    elif x.op.op=='not': return threevl.ThreeVL.nein(inner)
    else: raise NotImplementedError('unk_op',x.op)
  elif isinstance(x,sqparse2.NameX): return nix.rowget(tables,c_row,x)
  elif isinstance(x,sqparse2.AsterX):
    # todo doc: how does this get disassembled by caller?
    return sum(c_row,[])
  elif isinstance(x,sqparse2.ArrayLit): return map(subcall,x.vals)
  elif isinstance(x,(sqparse2.Literal,sqparse2.ArrayLit)): return x.toliteral()
  elif isinstance(x,sqparse2.CommaX):
    ret = []
    for child in x.children:
      (ret.extend if starlike(child) else ret.append)(subcall(child))
    return ret
  elif isinstance(x,sqparse2.CallX):
    print 'callx',x
    if is_aggregate(x): # careful: is_aggregate, not contains_aggregate
      if not isinstance(c_row,list): raise TypeError('aggregate function expected a list of rows')
      if len(x.args.children)!=1: raise ValueError('aggregate function expected a single value',x.args)
      arg,=x.args.children # intentional: error if len!=1
      vals=[evalex(arg,c_r,nix,tables) for c_r in c_row]
      if not vals: return None
      if x.f=='min': return min(vals)
      elif x.f=='max': return max(vals)
      else: raise NotImplementedError
    else:
      args=subcall(x.args)
      if x.f=='coalesce':
        a,b=args # todo: does coalesce take more than 2 args?
        return b if a is None else a
      else: raise NotImplementedError('unk_function',x.f.name)
  elif isinstance(x,sqparse2.SelectX): raise NotImplementedError('subqueries should have been evaluated earlier') # todo: better error class
  elif isinstance(x,sqparse2.AttrX):return nix.rowget(tables,c_row,x)
  elif isinstance(x,sqparse2.CaseX):
    for case in x.cases:
      if subcall(case.when): return subcall(case.then)
    return subcall(x.elsex)
  elif isinstance(x,(int,basestring,float,type(None))):
    return x # I think Table.insert is creating this in expand_row
  # todo doc: why tuple and list below?
  elif isinstance(x,tuple): return tuple(map(subcall, x))
  elif isinstance(x,list): return map(subcall, x)
  elif isinstance(x,sqparse2.NullX): return None
  elif isinstance(x,sqparse2.ReturnX):
    ret=subcall(x.expr)
    print 'ret:',ret,x.expr
    print "warning: not sure what I'm doing here with cardinality tweak on CommaX"
    return [ret] if isinstance(x.expr,(sqparse2.CommaX,sqparse2.AsterX)) else [[ret]] # todo: update parser so this is always * or a commalist
  else: raise NotImplementedError(type(x),x)

def sub_slots(x,match_fn,path=(),arr=None):
  "recursive. for each match found, add a tree-index tuple to arr"
  if arr is None: arr=[]
  if isinstance(x,sqparse2.BaseX):
    for attr in x.ATTRS:
      if attr in x.VARLEN:
        val = getattr(x,attr)
        for i,elt in enumerate(val or ()):
          nextpath = path + ((attr,i),)
          if match_fn(elt): arr.append(nextpath)
          sub_slots(elt,match_fn,nextpath,arr)
      else:
        nextpath = path + (attr,)
        val = getattr(x,attr)
        if match_fn(val): arr.append(nextpath)
        sub_slots(val,match_fn,nextpath,arr)
  return arr

def depth_first_sub(expr,values):
  "this *modifies in place* the passed-in expression, recursively replacing SubLit with literals"
  arr=sub_slots(expr,lambda elt:elt is sqparse2.SubLit)
  if len(arr)!=len(values): raise ValueError('len',len(arr),len(values))
  for path,val in zip(arr,values):
    if isinstance(val,(basestring,int,float)): expr[path] = sqparse2.Literal(val)
    elif isinstance(val,(list,tuple)): expr[path] = sqparse2.ArrayLit(val)
    else: raise TypeError('unk_sub_type',type(val),val)
  return expr
