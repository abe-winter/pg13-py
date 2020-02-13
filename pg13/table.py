"table -- Table class"

import collections
from . import pg, threevl, sqparse2, sqex

# errors
class PgExecError(sqparse2.PgMockError): "base class for errors during table execution"
class BadFieldName(PgExecError): pass
class IntegrityError(PgExecError): pass # careful: pgmock_dbapi also defines this

class Missing: "for distinguishing missing columns vs passed-in null"

def expand_row(table_fields,fields,values):
  "helper for insert. turn (field_names, values) into the full-width, properly-ordered row"
  table_fieldnames=[f.name for f in table_fields]
  reverse_indexes={table_fieldnames.index(f):i for i,f in enumerate(fields)}
  indexes=[reverse_indexes.get(i) for i in range(len(table_fields))]
  return [(Missing if i is None else values[i]) for i in indexes]

def emergency_cast(colx, value):
  """ugly: this is a huge hack. get serious about where this belongs in the architecture.
  For now, most types rely on being fed in as SubbedLiteral.
  """
  if colx.coltp.type.lower()=='boolean':
    if isinstance(value,sqparse2.NameX): value = value.name
    if isinstance(value,bool): return value
    return dict(true=True, false=False)[value.lower()] # keyerror if other
  else:
    return value # todo: type check?

def field_default(colx, table_name, tables_dict):
  "takes sqparse2.ColX, Table"
  if colx.coltp.type.lower() == 'serial':
    x = sqparse2.parse('select coalesce(max(%s),-1)+1 from %s' % (colx.name, table_name))
    return sqex.run_select(x, tables_dict, Table)[0]
  elif colx.not_null: raise NotImplementedError('todo: not_null error')
  else: return toliteral(colx.default)

FieldLookup=collections.namedtuple('FieldLookup','index type')
def toliteral(probably_literal):
  # todo: among the exception cases are Missing, str. go through cases and make this cleaner. the test suite alone has multiple types here.
  if probably_literal==sqparse2.NameX('null'): return None
  return probably_literal.toliteral() if hasattr(probably_literal,'toliteral') else probably_literal

class Table:
  def __init__(self,name,fields,pkey):
    "fields is a list of sqparse2.ColX"
    self.name,self.fields,self.pkey=name,fields,(pkey or [])
    self.rows=[]
    self.child_tables=[] # tables that inherit from this one
    self.parent_table=None # table this inherits from
  
  def get_column(self,name):
    col = next((f for f in self.fields if f.name==name), None)
    if col is None: raise KeyError(name)
    return col
  
  def pkey_get(self,row):
    if len(self.pkey):
      indexes=[i for i,f in enumerate(self.fields) if f.name in self.pkey]
      if len(indexes)!=len(self.pkey): raise ValueError('bad pkey')
      pkey_vals=list(map(row.__getitem__,indexes))
      return next((r for r in self.rows if pkey_vals==list(map(r.__getitem__,indexes))),None)
    else:
      # warning: is this right? it's saying that if not given, the pkey is the whole row. test dupe inserts on a real DB.
      return row if row in self.rows else None
  
  def fix_rowtypes(self,row):
    if len(row)!=len(self.fields): raise ValueError
    return list(map(toliteral,row))
  
  def apply_defaults(self, row, tables_dict):
    "apply defaults to missing cols for a row that's being inserted"
    return [
      emergency_cast(colx, field_default(colx, self.name, tables_dict) if v is Missing else v)
      for colx,v in zip(self.fields,row)
    ]
  
  def insert(self,fields,values,returning,tables_dict):
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    expanded_row=self.fix_rowtypes(expand_row(self.fields,fields,values) if fields else values)
    row=self.apply_defaults(expanded_row, tables_dict)
    # todo: check ColX.not_null here. figure out what to do about null pkey field
    for i,elt in enumerate(row):
      # todo(awinter): think about dependency model if one field relies on another. (what do I mean? 'insert into t1 (a,b) values (10,a+5)'? is that valid?)
      row[i]=sqex.Evaluator(row,nix,tables_dict).eval(elt)
    if self.pkey_get(row): raise pg.DupeInsert(row)
    self.rows.append(row)
    if returning: return sqex.Evaluator((row,),nix,tables_dict).eval(returning)
  
  def match(self,where,tables,nix):
    return [r for r in self.rows if not where or threevl.ThreeVL.test(sqex.Evaluator((r,),nix,tables).eval(where))]
  
  def lookup(self,name):
    if isinstance(name,sqparse2.NameX): name = name.name # this is horrible; be consistent
    try: return FieldLookup(*next((i,f) for i,f in enumerate(self.fields) if f.name==name))
    except StopIteration: raise BadFieldName(name)
  
  def update(self,setx,where,returning,tables_dict):
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    if not all(isinstance(x,sqparse2.AssignX) for x in setx): raise TypeError('not_xassign',list(map(type,setx)))
    match_rows=self.match(where,tables_dict,nix) if where else self.rows
    for row in match_rows:
      for x in setx: row[self.lookup(x.col).index]=sqex.Evaluator((row,),nix,tables_dict).eval(x.expr)
    if returning: return sqex.Evaluator((row,),nix,tables_dict).eval(returning)
  
  def delete(self,where,tables_dict):
    # todo: what's the deal with nested selects in delete. does it get evaluated once to a scalar before running the delete?
    # todo: this will crash with empty where clause
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    # todo(doc): why 'not' below?
    self.rows=[r for r in self.rows if not sqex.Evaluator((r,),nix,tables_dict).eval(where)]
