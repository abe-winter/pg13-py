"expression evaluation helpers for pgmock. has duck-dependencies on pgmock's Table class, needs redesign."
# todo: most of the heavy lifting happens here. profile and identify candidates for Cython port.

import itertools,collections
from . import sqparse2,threevl,misc

# todo: derive errors below from something pg13-specific
class ColumnNameError(StandardError): "name not in any tables or name matches too many tables"
class TableNameError(StandardError): "expression referencing unk table"

# todo doc: explain why it's not necessary to do these checks on the whereclause
def consumes_rows(ex): return isinstance(ex,sqparse2.CallX) and ex.f in ('min','max','count')
def returns_rows(ex): return isinstance(ex,sqparse2.CallX) and ex.f in ('unnest',)
def contains(ex,f): return bool(sub_slots(ex,f,match=True))

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
    # todo: support a TextSearchDoc that will overload a lot of these operators
    if not all(isinstance(x,list) for x in (left,right)): raise TypeError('non-array-args',op,left,right)
    return set(left)>=set(right)
  elif op=='||':
    if not all(isinstance(x,list) for x in (left,right)): raise TypeError('non-array-args',op,left,right)
    return left+right
  elif op=='@@':
    if not all(isinstance(x,set) for x in (left,right)): raise TypeError('non_set_args',op,type(left),type(right))
    return bool(left&right)
  else: raise NotImplementedError(op,left,right) # pragma: no cover

def uniqify(list_):
  "inefficient on long lists; short lists only. preserves order."
  a=[]
  for x in list_:
    if x not in a: a.append(x)
  return a

def eliminate_sequential_children(paths):
  "helper for infer_columns. removes paths that are direct children of the n-1 or n-2 path"
  return [p for i,p in enumerate(paths) if not ((i>0 and paths[i-1]==p[:-1]) or (i>1 and paths[i-2]==p[:-1]))]

def infer_columns(selectx,tables_dict):
  """infer the columns for a subselect that creates an implicit table.
  the output of this *can* contain duplicate names, fingers crossed that downstream code uses the first.
  (Look up SQL spec on dupe names.)
  todo(refactor): I think there's common logic here and inside NameIndexer that can be merged.
  todo(ugly): this is a beast
  """
  # todo: support CTEs -- with all this plumbing, might as well
  table2fields = {}
  table_order = []
  for t in selectx.tables:
    if isinstance(t,basestring):
      table2fields[t]=tables_dict[t].fields
      table_order.append(t)
    elif isinstance(t,sqparse2.AliasX):
      if isinstance(t.name,basestring):
        table2fields[t]=tables_dict[t]
        table_order.append(t.name)
      elif isinstance(t.name,sqparse2.SelectX): raise NotImplementedError('todo: inner subquery')
      else: raise TypeError('AliasX.name',type(t.name))
    else: raise TypeError('table',type(t))
  # the forms are: *, x.*, x.y, y. expressions are anonymous unless they have an 'as' (which I don't support)
  table_order=uniqify(table_order)
  cols=[]
  used_name_collision = collections.Counter()
  for col in selectx.cols.children:
    if isinstance(col,sqparse2.AsterX):
      for t in table_order:
        cols.extend(table2fields[t])
    elif isinstance(col,sqparse2.BaseX):
      all_paths = sub_slots(col, lambda x:isinstance(x,(sqparse2.AttrX,sqparse2.NameX,sqparse2.AliasX)), match=True)
      paths = eliminate_sequential_children(all_paths) # this eliminates NameX under AttrX
      for p in paths:
        x = col[p]
        if isinstance(x,sqparse2.AttrX):
          if not isinstance(x.parent,sqparse2.NameX): raise TypeError('parent_not_name',type(x.parent))
          if isinstance(x.attr,sqparse2.NameX): raise NotImplementedError # todo
          elif isinstance(x.attr,sqparse2.AsterX): cols.extend(table2fields[x.parent.name])
          else: raise TypeError('attr_unk_type',type(x.attr))
        elif isinstance(x,sqparse2.NameX):
          matching_fields = filter(None,(next((f for f in table2fields[t] if f.name==x.name),None) for t in table_order))
          if len(matching_fields)!=1: raise sqparse2.SQLSyntaxError('missing_or_dupe_field',x,matching_fields)
          cols.append(matching_fields[0])
        elif isinstance(x,sqparse2.AliasX): cols.append(sqparse2.ColX(x.alias,None,None,None,None,None))
        else: raise TypeError('unk_item_type',type(x)) # pragma: no cover
    else: raise TypeError('unk_col_type',type(col)) # pragma: no cover
  return cols

class NameIndexer:
  """helper that takes str, NameX or attrx and returns the right thing (or raises an error on ambiguity).
  Note: alias-only tables 'select from (nested select) as alias' currently live here. replace_subqueries might be a better place.
  warning: a-only tables probably need to work out their dependency graph
  """
  @staticmethod
  def update_aliases(aliases,aonly,x):
    "helper for ctor. takes AliasX or string as second arg"
    if isinstance(x,basestring): aliases[x]=x
    elif isinstance(x,sqparse2.AliasX):
      if not isinstance(x.alias,basestring): raise TypeError('alias not string',type(x.alias))
      if isinstance(x.name,sqparse2.NameX): aliases.update({x.alias:x.name.name,x.name.name:x.name.name})
      elif isinstance(x.name,sqparse2.SelectX):
        aliases.update({x.alias:x.alias})
        aonly[x.alias]=x.name
      else: raise TypeError('aliasx_unk_thing',type(x.name)) # pragma: no cover
    else: raise TypeError(type(x)) # pragma: no cover
  @classmethod
  def ctor_fromlist(clas,fromlistx):
    aliases={}
    aonly={}
    for from_item in fromlistx:
      if isinstance(from_item,basestring): clas.update_aliases(aliases,aonly,from_item)
      elif isinstance(from_item,sqparse2.AliasX): clas.update_aliases(aliases,aonly,from_item)
      elif isinstance(from_item,sqparse2.JoinX):
        clas.update_aliases(aliases,aonly,from_item.a)
        clas.update_aliases(aliases,aonly,from_item.b)
      else: raise TypeError(type(from_item)) # pragma: no cover
    table_order=sorted(set(aliases.values()))
    return clas(aliases,table_order,aonly)
  @classmethod
  def ctor_name(clas,name): return clas({name:name},[name],{})
  def __init__(self,aliases,table_order,alias_only_tables):
    self.aliases,self.table_order,self.aonly = aliases,table_order,alias_only_tables
    self.aonly_resolved=False
  @misc.meth_once
  def resolve_aonly(self,tables_dict,table_ctor):
    "circular depends on pgmock.Table. refactor."
    for alias,selectx in self.aonly.items():
      table = table_ctor(alias,infer_columns(selectx,tables_dict),None)
      table.rows = run_select(selectx,tables_dict,table_ctor)
      self.aonly[alias] = table
    self.aonly_resolved = True
  def index_tuple(self,tables_dict,index,is_set):
    "helper for rowget/rowset"
    if not self.aonly_resolved: raise RuntimeError('resolve_aonly() before querying nix')
    with tables_dict.tempkeys():
      tables_dict.update(self.aonly) # todo: find spec support for aliases overwriting existing tables. (more likely, it's an error)
      index = index.name if isinstance(index,sqparse2.NameX) else index
      if isinstance(index,basestring):
        candidates = [t for t in self.table_order if any(f.name==index for f in tables_dict[t].fields)]
        if len(candidates)!=1: raise ColumnNameError(("ambiguous_column" if candidates else "no_such_column"),index)
        tname, = candidates
        return self.table_order.index(tname), tables_dict[tname].lookup(index).index
      elif isinstance(index,sqparse2.AttrX):
        if index.parent.name not in self.aliases: raise TableNameError('table_notin_x',index.parent,self.aliases)
        tname = self.aliases[index.parent.name]
        tindex = self.table_order.index(tname)
        if isinstance(index.attr,sqparse2.AsterX):
          if is_set: raise ValueError('cant_set_asterisk') # todo: better error class
          else: return (tindex,)
        else: return (tindex,tables_dict[tname].lookup(index.attr).index)
        # todo: stronger typing here. make sure both fields of the AttrX are always strings.
      else: raise TypeError(type(index)) # pragma: no cover
  def rowget(self,tables_dict,row_list,index):
    "row_list in self.row_order"
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

def replace_subqueries(ex,tables,table_ctor):
  "this mutates passed in ex (any BaseX), replacing nested selects with their (flattened) output"
  # http://www.postgresql.org/docs/9.1/static/sql-expressions.html#SQL-SYNTAX-SCALAR-SUBQUERIES
  # see here for subquery conditions that *do* use multi-rows. ug. http://www.postgresql.org/docs/9.1/static/functions-subquery.html
  if isinstance(ex,sqparse2.SelectX):
    old_tables, ex.tables = ex.tables, [] # we *don't* recurse into tables because selects in here get transformed into tables
  for path in sub_slots(ex, lambda x:isinstance(x,sqparse2.SelectX)):
    ex[path] = sqparse2.Literal(flatten_scalar(run_select(ex[path], tables, table_ctor)))
  if isinstance(ex,sqparse2.SelectX): ex.tables = old_tables
  return ex # but it was modified in place, too

def unnest_helper(cols,row):
  wrapped = [val if contains(col,returns_rows) else [val] for col,val in zip(cols.children,row)]
  return map(list,itertools.product(*wrapped))

def collapse_group_expr(groupx,cols,ret_row):
  "collapses columns matching the group expression. I'm sure this is buggy; look at a real DB's imp of this."
  for i,col in enumerate(cols.children):
    if col==groupx: ret_row[i]=ret_row[i][0]
  return ret_row

def run_select(ex,tables,table_ctor):
  nix, where = decompose_select(ex)
  nix.resolve_aonly(tables,table_ctor)
  with tables.tempkeys(): # so aliases are temporary. todo doc: why am I doing this here *and* in index_tuple?
    tables.update(nix.aonly)
    composite_rows = [c_row for c_row in itertools.product(*(tables[t].rows for t in nix.table_order)) if eval_where(where,c_row,nix,tables)]
    if ex.order: # note: order comes before limit / offset
      composite_rows.sort(key=lambda c_row:evalex(ex.order,c_row,nix,tables))
    if ex.limit or ex.offset: # pragma: no cover
      print ex.limit, ex.offset
      raise NotImplementedError('notimp: limit,offset,group')
    if ex.group:
      # todo: non-aggregate expressions are allowed if they consume only the group expression
      # todo: does the group expression have to be a NameX? for now it can be any expression. check specs.
      # todo: this block shares logic with other parts of the function. needs refactor.
      badcols=[col for col in ex.cols.children if not col==ex.group and not contains(col,consumes_rows)]
      if badcols: raise ValueError('illegal_cols_in_group',badcols)
      if contains(ex.cols,returns_rows): raise NotImplementedError('todo: unnest with grouping')
      groups = collections.OrderedDict()
      for row in composite_rows:
        k = evalex(ex.group,row,nix,tables)
        if k not in groups: groups[k] = []
        groups[k].append(row)
      return [collapse_group_expr(ex.group, ex.cols, evalex(ex.cols,g_rows,nix,tables)) for g_rows in groups.values()]
    if contains(ex.cols,consumes_rows):
      if not all(contains(col,consumes_rows) for col in ex.cols.children):
        # todo: this isn't good enough. what about nesting cases like max(min(whatever))
        raise sqparse2.SQLSyntaxError('not_all_aggregate') # is this the way real PG works? aim at giving PG error codes
      return evalex(ex.cols,composite_rows,nix,tables)
    else:
      ret = [evalex(ex.cols,r,nix,tables) for r in composite_rows]
      return sum((unnest_helper(ex.cols,row) for row in ret),[]) if contains(ex.cols,returns_rows) else ret

def starlike(x):
  "weird things happen to cardinality when working with * in comma-lists. this detects when to do that."
  # todo: is '* as name' a thing?
  return isinstance(x,sqparse2.AsterX) or isinstance(x,sqparse2.AttrX) and isinstance(x.attr,sqparse2.AsterX)

# todo: evalex should use intermediate types: Scalar, Row, RowList, Table.
#   Row and Table might be able to bundle into RowList. RowList should know the type and names of its columns.
#   This will solve a lot of cardinality confusion.
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
    else: raise NotImplementedError('unk_op',x.op) # pragma: no cover
  elif isinstance(x,sqparse2.NameX): return nix.rowget(tables,c_row,x)
  elif isinstance(x,sqparse2.AsterX):
    # todo doc: how does this get disassembled by caller?
    return sum(c_row,[])
  elif isinstance(x,sqparse2.ArrayLit): return map(subcall,x.vals)
  elif isinstance(x,(sqparse2.Literal,sqparse2.ArrayLit)): return x.toliteral()
  elif isinstance(x,sqparse2.CommaX):
    # todo: think about getting rid of CommaX everywhere; it complicates syntax tree navigation.
    ret = []
    for child in x.children:
      (ret.extend if starlike(child) else ret.append)(subcall(child))
    return ret
  elif isinstance(x,sqparse2.CallX):
    if consumes_rows(x): # this isn't contains(x,consumes_row) -- it's just checking the current expression
      if not isinstance(c_row,list): raise TypeError('aggregate function expected a list of rows')
      if len(x.args.children)!=1: raise ValueError('aggregate function expected a single value',x.args)
      arg,=x.args.children # intentional: error if len!=1
      vals=[evalex(arg,c_r,nix,tables) for c_r in c_row]
      if not vals: return None
      if x.f=='min': return min(vals)
      elif x.f=='max': return max(vals)
      elif x.f=='count': return len(vals)
      else: raise NotImplementedError('unk_func',x.f) # pragma: no cover
    else:
      # todo: get more concrete about argument counts
      args=subcall(x.args)
      if x.f=='coalesce':
        a,b=args # todo: does coalesce take more than 2 args?
        return b if a is None else a
      elif x.f=='unnest': return subcall(x.args)[0] # note: run_select does some work in this case too
      elif x.f in ('to_tsquery','to_tsvector'): return set(subcall(x.args.children[0]).split())
      else: raise NotImplementedError('unk_function',x.f) # pragma: no cover
  elif isinstance(x,sqparse2.SelectX): raise NotImplementedError('subqueries should have been evaluated earlier') # todo: better error class
  elif isinstance(x,sqparse2.AttrX):return nix.rowget(tables,c_row,x)
  elif isinstance(x,sqparse2.CaseX):
    for case in x.cases:
      if subcall(case.when): return subcall(case.then)
    return subcall(x.elsex)
  elif isinstance(x,sqparse2.CastX):
    if x.to_type.type.lower() in ('text','varchar'): return unicode(subcall(x.expr))
    else: raise NotImplementedError('unhandled_cast_type',x.to_type)
  elif isinstance(x,(int,basestring,float,type(None))):
    return x # I think Table.insert is creating this in expand_row
  # todo: why tuple, list, dict below? throw some asserts in here and see where these are coming from.
  elif isinstance(x,tuple): return tuple(map(subcall, x))
  elif isinstance(x,list): return map(subcall, x)
  elif isinstance(x,dict): return x
  elif isinstance(x,sqparse2.NullX): return None
  elif isinstance(x,sqparse2.ReturnX):
    # todo: I think ReturnX is *always* CommaX now; revisit this
    ret=subcall(x.expr)
    print "warning: not sure what I'm doing here with cardinality tweak on CommaX"
    return [ret] if isinstance(x.expr,(sqparse2.CommaX,sqparse2.AsterX)) else [[ret]] # todo: update parser so this is always * or a commalist
  elif isinstance(x,sqparse2.AliasX): return subcall(x.name) # todo: rename AliasX 'name' to 'expr'
  else: raise NotImplementedError(type(x),x) # pragma: no cover

def sub_slots(x,match_fn,path=(),arr=None,match=False): # todo: rename match to topmatch for clarity
  """given a BaseX in x, explore its ATTRS (doing the right thing for VARLEN).
  return a list of tree-paths (i.e. tuples) for tree children that match match_fn. The root elt won't match.
  """
  # todo: profiling suggests this getattr-heavy recursive process is the next bottleneck
  if arr is None: arr=[]
  if match and match_fn(x): arr.append(path)
  if isinstance(x,sqparse2.BaseX):
    for attr in x.ATTRS:
      val = getattr(x,attr)
      if attr in x.VARLEN:
        for i,elt in enumerate(val or ()):
          nextpath = path + ((attr,i),)
          sub_slots(elt,match_fn,nextpath,arr,True)
      else:
        nextpath = path + (attr,)
        sub_slots(val,match_fn,nextpath,arr,True)
  return arr

def depth_first_sub(expr,values):
  "replace SubLit with literals in expr. (expr is mutated)."
  arr=sub_slots(expr,lambda elt:elt is sqparse2.SubLit)
  if len(arr)!=len(values): raise ValueError('len',len(arr),len(values))
  for path,val in zip(arr,values):
    # todo: does ArrayLit get us anything? tree traversal?
    if isinstance(val,(basestring,int,float,type(None),dict)): expr[path] = sqparse2.Literal(val)
    elif isinstance(val,(list,tuple)): expr[path] = sqparse2.ArrayLit(val)
    else: raise TypeError('unk_sub_type',type(val),val) # pragma: no cover
  return expr
