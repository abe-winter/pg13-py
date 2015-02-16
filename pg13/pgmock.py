"table class and apply_sql. this is weirdly codependent with sqex.py"

import re,collections,contextlib
from . import pg,threevl,sqparse2,sqex

# errors
class PgExecError(sqparse2.PgMockError): "base class for errors during table execution"
class MissingPKeyError(PgExecError): pass
class BadFieldName(PgExecError): pass

class Missing: "for distinguishing missing columns vs passed-in null"

def expand_row(table_fields,fields,values):
  "helper for insert. turn (field_names, values) into the full-width, properly-ordered row"
  table_fieldnames=[f.name for f in table_fields]
  reverse_indexes={table_fieldnames.index(f):i for i,f in enumerate(fields)}
  indexes=[reverse_indexes.get(i) for i in range(len(table_fields))]
  return [(Missing if i is None else values[i]) for i in indexes]

FieldLookup=collections.namedtuple('FieldLookup','index type')
def toliteral(probably_literal):
  # todo: among the exception cases are Missing, str. go through cases and make this cleaner. the test suite alone has multiple types here.
  if probably_literal==sqparse2.NameX('null'): return None
  return probably_literal.toliteral() if hasattr(probably_literal,'toliteral') else probably_literal
class Table:
  def __init__(self,name,fields,pkey): self.name,self.fields,self.pkey=name,fields,(pkey or []); self.rows=[]
  def pkey_get(self,row):
    if len(self.pkey):
      indexes=[i for i,f in enumerate(self.fields) if f.name in self.pkey]
      if len(indexes)!=len(self.pkey): raise ValueError('bad pkey')
      pkey_vals=map(row.__getitem__,indexes)
      return next((r for r in self.rows if pkey_vals==map(r.__getitem__,indexes)),None)
    else: return row if row in self.rows else None
  def fix_rowtypes(self,row):
    if len(row)!=len(self.fields): raise ValueError
    return map(toliteral,row)
  def apply_defaults(self,row):
    "apply defaults to missing cols for a row that's being inserted"
    return [(toliteral(f.default) if v is Missing else v) for f,v in zip(self.fields,row)]
  def insert(self,fields,values,returning,tables_dict):
    nix = sqex.NameIndexer.ctor_name(self.name)
    expanded_row=self.fix_rowtypes(expand_row(self.fields,fields,values) if fields else values)
    row=self.apply_defaults(expanded_row)
    # todo: check ColX.not_null here. figure out what to do about null pkey field
    for i,elt in enumerate(row):
      # todo(awinter): think about dependency model if one field relies on another. (what do I mean? 'insert into t1 (a,b) values (10,a+5)'? is that valid?)
      row[i]=sqex.evalex(elt,row,nix,tables_dict)
    if self.pkey_get(row): raise pg.DupeInsert(row)
    self.rows.append(row)
    if returning: return sqex.evalex(returning,(row,),nix,tables_dict)
  def match(self,where,tables,nix):
    return [r for r in self.rows if not where or threevl.ThreeVL.test(sqex.evalex(where,(r,),nix,tables))]
  def lookup(self,name):
    if isinstance(name,sqparse2.NameX): name = name.name # this is horrible; be consistent
    try: return FieldLookup(*next((i,f) for i,f in enumerate(self.fields) if f.name==name))
    except StopIteration: raise BadFieldName(name)
  def returnrows(self,tables,fields,rows):
    raise NotImplementedError('returnrows nix')
    return [sqex.evalex(fields,row,self.name,tables) for row in rows]
  def select(self,fields,whereclause,tables_dict,order):
    nix = sqex.NameIndexer.ctor_name(self.name)
    match_rows=self.match(whereclause,tables_dict,nix)
    if list(fields.children)==['*']: return self.order_rows(match_rows,order,tables_dict)
    elif sqex.contains_aggregate(fields):
      if not all(map(sqex.contains_aggregate,fields.children)): raise sqparse2.SQLSyntaxError('not_all_aggregate') # is this the way real PG works? aim at giving PG error codes
      return self.order_rows([sqex.evalex(f,match_rows,nix,tables_dict) for f in fields.children],order,tables_dict)
    else: return self.order_rows(self.returnrows(tables_dict,fields,match_rows),order,tables_dict)
  def update(self,setx,where,returning,tables_dict):
    nix = sqex.NameIndexer.ctor_name(self.name)
    if not all(isinstance(x,sqparse2.AssignX) for x in setx): raise TypeError('not_xassign',map(type,setx))
    match_rows=self.match(where,tables_dict,nix) if where else self.rows
    for row in match_rows:
      for x in setx: row[self.lookup(x.col).index]=sqex.evalex(x.expr,(row,),nix,tables_dict)
    if returning: return sqex.evalex(returning,(row,),nix,tables_dict)
  def delete(self,where,tables_dict):
    # todo: what's the deal with nested selects in delete. does it get evaluated once to a scalar before running the delete?
    # todo: this will crash with empty where clause
    nix = sqex.NameIndexer.ctor_name(self.name)
    self.rows=[r for r in self.rows if not sqex.evalex(where,(r,),nix,tables_dict)]

def apply_sql(ex,values,tables_dict):
  "call the stmt in tree with values subbed on the tables in t_d\
  tree is a parsed statement returned by parse_expression. values is the tuple of %s replacements. tables_dict is a dictionary of Table instances."
  sqex.depth_first_sub(ex,values)
  sqex.replace_subqueries(ex,tables_dict)
  if isinstance(ex,sqparse2.SelectX): return sqex.run_select(ex,tables_dict)
  elif isinstance(ex,sqparse2.InsertX): return tables_dict[ex.table].insert(ex.cols,ex.values,ex.ret,tables_dict)
  elif isinstance(ex,sqparse2.UpdateX):
    if len(ex.tables)!=1: raise NotImplementedError('multi-table update')
    return tables_dict[ex.tables[0]].update(ex.assigns,ex.where,ex.ret,tables_dict)
  elif isinstance(ex,sqparse2.CreateX):
    if ex.name in tables_dict: raise ValueError('table_exists',ex.name)
    if any(c.pkey for c in ex.cols): raise NotImplementedError('inline pkey')
    tables_dict[ex.name]=Table(ex.name,ex.cols,ex.pkey.fields if ex.pkey else [])
  elif isinstance(ex,sqparse2.IndexX): pass
  elif isinstance(ex,sqparse2.DeleteX): return tables_dict[ex.table].delete(ex.where,tables_dict)
  else: raise TypeError(type(ex))

class ConnectionMock:
  "for supporting the contextmanager call"
  def __init__(self,poolmock): self.poolmock=poolmock
  @contextlib.contextmanager
  def cursor(self): yield self.poolmock

class PgPoolMock(pg.PgPool): # only inherits so isinstance tests pass
  def __init__(self): self.tables={}
  def select(self,qstring,vals=()): return apply_sql(sqparse2.parse(qstring),vals,self.tables)
  def commit(self,qstring,vals=()): return apply_sql(sqparse2.parse(qstring),vals,self.tables)
  def commitreturn(self,qstring,vals=()): return apply_sql(sqparse2.parse(qstring),vals,self.tables)[0]
  def close(self): self.tables={} # so GC can work. doubt this will ever get called.
  @contextlib.contextmanager
  def __call__(self): yield ConnectionMock(self)
