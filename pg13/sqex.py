"expression evaluation helpers for pgmock. has duck-dependencies on pgmock's Table class, needs redesign."

import functools,itertools
from . import sqparse,threevl

# todo: derive errors below from something pg13-specific
class ColumnNameError(StandardError): "name not in any tables or name matches too many tables"
class TableNameError(StandardError): "expression referencing unk table"

def is_aggregate(ex):
  "is the expression something that runs on a list of rows rather than a single row"
  # todo: I think the SQL term for this is 'scalar subquery'. use SQL vocabulary
  # todo doc: explain why it's not necessary to do this check on the whereclause
  return isinstance(ex,sqparse.CallX) and ex.f in (sqparse.NameX('min'),sqparse.NameX('max'))
def contains_aggregate(ex): return ex.treeany(is_aggregate)

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
    for from_item in fromlistx.fromlist:
      if isinstance(from_item,sqparse.FromTableX): clas.update_aliases(aliases,from_item)
      elif isinstance(from_item,sqparse.JoinX):
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
    if isinstance(index,sqparse.NameX): index = index.name
    # careful below: intentionally if, not elif
    if isinstance(index,basestring):
      candidates = [t for t in self.table_order if any(f.name.name==index for f in tables_dict[t].fields)]
      if len(candidates)!=1: raise ColumnNameError(("ambiguous_column" if candidates else "no_such_column"),index)
      tname, = candidates
      return self.table_order.index(tname), tables_dict[tname].lookup(index).index
    if isinstance(index,sqparse.AttrX):
      if not index.parent in self.aliases: raise TableNameError('table_notin_x',index.parent)
      tindex = self.table_order.index(self.aliases[index.parent])
      if index.attr=='*':
        if is_set: raise ValueError('cant_set_asterisk') # todo: better error class
        else: return (tindex,)
      else: return (tindex,tables_dict[self.aliases[index.parent]].lookup(index.attr).index)
      # todo: stronger typing here. make sure both fields of the AttrX are always strings.
    else: raise TypeError(type(index))
  def rowget(self,tables_dict,row_list,index):
    "row_list in self.row_order"
    tmp=row_list
    for i in self.index_tuple(tables_dict,index,False): tmp=tmp[i]
    return tmp
  def rowset(self,tables_dict,row_list,index): raise NotImplementedError # note: shouldn't need this until update uses this
  def __repr__(self): return '<NameIndexer %s>'%self.table_order

def decompose_select(selectx):
  "return [(parent,setter) for scalar_subquery], wherex_including_on, NameIndexer. helper for run_select"
  subqueries = sub_slots(selectx, lambda x:isinstance(x,sqparse.SelectX))
  nix = NameIndexer.ctor_fromlist(selectx.tables)
  where = []
  for fromx in selectx.tables.fromlist:
    if isinstance(fromx,sqparse.JoinX) and fromx.on_stmt is not None:
      # todo: what happens if on_stmt columns are non-ambiguous in the context of the join tables but ambiguous overall? yuck.
      where.append(fromx.on_stmt)
  if selectx.where: where.append(selectx.where)
  return subqueries, nix, where

def eval_where(where_list,composite_row,nix,tables_dict):
  "join-friendly whereclause evaluator. composite_row is a list or tuple of row lists. where_list is the thing from decompose_select."
  # todo: do I need to use 3vl instead of all() to merge where_list?
  return all(evalex(w,c_row,nix,tables_dict) for w in where_list)

def run_select(ex,tables):
  subqueries, nix, where = decompose_select(ex)
  for path in subqueries:
    print 'subq path',path
    raise NotImplementedError('replace subquery with value')
  print 'torder',nix.table_order
  composite_rows = [c_row for c_row in itertools.product(*(tables[t].rows for t in nix.table_order)) if eval_where(where,c_row,nix,tables)]
  print 'composite_rows',composite_rows
  if ex.limit or ex.offset:
    print ex.limit, ex.offset
    raise NotImplementedError('notimp: limit,offset')
  if contains_aggregate(ex.cols):
    raise NotImplementedError
    """
    if not all(map(sqex.contains_aggregate,fields.children)): raise sqparse.SQLSyntaxError('not_all_aggregate') # is this the way real PG works? aim at giving PG error codes
    return self.order_rows([sqex.evalex(f,match_rows,self.name,tables_dict) for f in fields.children],order,tables_dict)
    """
  else: return [evalex(ex.cols,r,nix,tables) for r in composite_rows]
  # todo above: take a sum-of-lists approach to joining columns because it also works for *. i.e. sum(([4],[7],[1,2,3]),[])

def evalex(x,c_row,nix,tables):
  "c_row is a composite row, i.e. a list/tuple of rows from all the query's tables, ordered by nix.table_order"
  def subcall(x): "helper for recursive calls"; return evalex(x,c_row,nix,tables)
  if not isinstance(nix, NameIndexer): raise NotImplementedError # todo: remove this once Table meths are all up-to-date
  if isinstance(x,sqparse.BinX):
    l,r=map(subcall,(x.left,x.right))
    return evalop(x.op.op,l,r)
  elif isinstance(x,sqparse.UnX):
    inner=subcall(x.val)
    if x.op.op=='+': return inner
    elif x.op.op=='-': return -inner
    elif x.op.op=='not': return threevl.ThreeVL.nein(inner)
    else: raise NotImplementedError('unk_op',x.op)
  elif isinstance(x,sqparse.NameX): return None if x.name=='null' else nix.rowget(tables,c_row,x)
  elif isinstance(x,sqparse.ArrayLit): return map(subcall,x.vals)
  elif isinstance(x,(sqparse.Literal,sqparse.ArrayLit)): return x.toliteral()
  elif isinstance(x,sqparse.CommaX): return map(subcall,x.children)
  elif isinstance(x,sqparse.CallX):
    raise NotImplementedError('not sure how this works yet with new args')
    if is_aggregate(x): # careful: is_aggregate, not contains_aggregate
      if not isinstance(row,list): raise TypeError('aggregate function expected a list of rows')
      if len(x.args.children)!=1: raise ValueError('aggregate function expected a single value',x.args)
      arg,=x.args.children
      vals=[evalex(arg,r,tablename,tables) for r in row]
      if not vals: return None
      if x.f.name=='min': return min(vals)
      elif x.f.name=='max': return max(vals)
      else: raise NotImplementedError
    else:
      args=evalex(x.args,row,tablename,tables)
      if x.f.name=='coalesce':
        a,b=args # todo: does coalesce take more than 2 args?
        return b if a is None else a
      else: raise NotImplementedError('unk_function',x.f.name)
  elif isinstance(x,sqparse.SelectX):
    raise NotImplementedError('subqueries should have been evaluated earlier') # todo: better error class
    # http://www.postgresql.org/docs/9.1/static/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES
    # see here for subquery conditions that *do* use multi-rows. ug. http://www.postgresql.org/docs/9.1/static/functions-subquery.html
    if len(x.cols.children)!=1: raise sqparse.SQLSyntaxError('scalar_subquery_requires_1col',len(x.cols.children))
    inner_rows=run_select(x,tables)
    if not inner_rows: return None
    if not isinstance(inner_rows[0],list): return inner_rows[0] # this happens with aggregate functions. todo: spec-compliance.
    (val,),=inner_rows # todo: raise an explicit error if len(inner_rows)!=1
    print 'inner select returning',val
    return val
  elif isinstance(x,sqparse.CaseX):
    for case in x.cases:
      if subcall(case.when): return subcall(case.then)
    return subcall(x.elsex)
  elif isinstance(x,(int,basestring,float,type(None))): return x # I think Table.insert is creating this in expand_row
  # todo doc: why tuple and list below?
  elif isinstance(x,tuple): return tuple(map(subcall, x))
  elif isinstance(x,list): return map(subcall, x)
  elif isinstance(x,sqparse.ReturnX):
    raise NotImplementedError('fix "returning" logic for c_row')
    if x.expr=='*': return row
    ret=subcall(x.expr)
    return ret if isinstance(x.expr,sqparse.CommaX) else [ret] # todo: update parser so this is always * or a commalist
  else: raise NotImplementedError(type(x),x)

SUBSLOT_ATTRS=[
  (sqparse.BinX, ('left','right')),
  (sqparse.AssignX, ('expr',)), # todo: can the assign-to name be subbed?
  (sqparse.UnX, ('val',)),
  (sqparse.CallX, ('args',)), # this is a CommaX; it will never be a SubLit, so it will always descend down. todo: can function name be subbed?
  (sqparse.ReturnX, ('expr',)),
  (sqparse.InsertX, ('table','cols','values','ret')),
  (sqparse.CreateX, ('name','cols','pkey')),
  (sqparse.UpdateX, ('tables','assigns','where','ret')),
  (sqparse.SelectX, ('cols','tables','where','order','limit','offset')),
  (sqparse.CaseX, ('elsex',)),
  (sqparse.WhenX, ('when','then')),
  (sqparse.NameX, ('name',)),
  (sqparse.AttrX, ('parent','attr')),
  (sqparse.OpX, ('optype','op')),
  (sqparse.DeleteX, ('table','where')),
  (sqparse.JoinX, ('a','b','on_stmt')),
  (sqparse.FromTableX, ('name','alias')),
  (sqparse.Literal, ('val',)),
]
VARLEN_ATTRS=[
  (sqparse.ArrayLit,('vals',)),
  (sqparse.CommaX,('children',)),
  (sqparse.CaseX, ('cases',)),
  (sqparse.FromListX,('fromlist',)),
  (sqparse.ArrayLit, ('vals',)),
]
def sub_slots(x,match_fn,path=(),arr=None):
  "recursive. for each match found, add a tree-index tuple to arr"
  if arr is None: arr=[]
  for clas,attrs in VARLEN_ATTRS:
    if isinstance(x,clas):
      for attr in attrs:
        for i,elt in enumerate(getattr(x,attr)):
          nextpath = path + ((attr,i),)
          if match_fn(elt): arr.append(nextpath)
          sub_slots(elt,match_fn,nextpath,arr)
      break # note: don't optimize this out. CaseX has subslot and varlen attrs
  for clas,attrs in SUBSLOT_ATTRS:
    if isinstance(x,clas):
      for attr in attrs:
        nextpath = path + (attr,)
        if match_fn(getattr(x,attr)): arr.append(nextpath)
        sub_slots(getattr(x,attr),match_fn,nextpath,arr)
      break
  return arr

def depth_first_sub(expr,values):
  "this *modifies in place* the passed-in expression, recursively replacing SubLit with literals"
  arr=sub_slots(expr,lambda elt:elt is sqparse.SubLit)
  if len(arr)!=len(values): raise ValueError('len',len(arr),len(values))
  for path,val in zip(arr,values):
    if isinstance(val,(basestring,int,float)): expr[path] = sqparse.Literal(val)
    elif isinstance(val,(list,tuple)): expr[path] = sqparse.ArrayLit(val)
    else: raise TypeError('unk_sub_type',type(val),val)
  return expr
