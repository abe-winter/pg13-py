"table -- Table class"

import collections
from . import pg, threevl, sqparse2

class UnkTableError(StandardError): "for lookup failures in composite rows"

class Composite: "use this for RowSource.table when a Row is composite"

class RowSource:
  "for things like update and delete we need to know where a row came from. this stores that."
  def __init__(self, table, index):
    "table is a table.Table or a scope.SyntheticTable"
    self.table, self.index = table, index

  @property
  def name(self):
    if isinstance(self.table, Table): return self.table.name
    elif self.table is Composite: return None
    else: raise TypeError(type(self.table), self.table)

class Row:
  """this is used for intermediate representation of a row during computation.
  Table.rows are stored as lists or tuples or something.
  """
  def __init__(self, source, vals):
    "source is a RowSource or None if it isn't from a table"
    self.source, self.vals = source, vals

  def get_table(self, name):
    "a Row can be composite (i.e. nest others under itself). This returns the nested Row (or self) with matching name."
    if self.source.name == name: return self
    return next(
      (val.get_table(name) for val in self.vals if isinstance(val, Row) and val.get_table(name) is not None),
      None
    )

  def index(self, column_name):
    if self.source.table is Composite:
      raise TypeError("can't index into composite row")
    return [f.name for f in self.source.table.fields].index(column_name)

  def __getitem__(self, (table_name, column_name)):
    actual_row = self.get_table(table_name)
    if not actual_row:
      raise UnkTableError(table_name, self)
    return actual_row.vals[actual_row.index(column_name)]

  def __repr__(self): return '<Row %s:%i %r>' % (self.source.name or 'composite', self.source.index, self.vals)

# errors
class PgExecError(sqparse2.PgMockError): "base class for errors during table execution"
class BadFieldName(PgExecError): pass
class IntegrityError(PgExecError): pass # careful: pgmock_dbapi also defines this

class Missing: "for distinguishing missing columns vs passed-in null"

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
  raise NotImplementedError("this can't import sqex")
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
  def __init__(self, name, fields, pkey):
    "fields is a list of sqparse2.ColX"
    self.name,self.fields,self.pkey=name,fields,(pkey or [])
    self.rows=[]
    self.child_tables=[] # tables that inherit from this one
    self.parent_table=None # table this inherits from
  
  def to_rowlist(self):
    "return [Row, ...] for intermediate computations"
    return [Row(RowSource(self, i), row) for i, row in enumerate(self.rows)]

  def get_column(self,name):
    col = next((f for f in self.fields if f.name==name), None)
    if col is None: raise KeyError(name)
    return col
  
  def pkey_get(self,row):
    if len(self.pkey):
      indexes=[i for i,f in enumerate(self.fields) if f.name in self.pkey]
      if len(indexes)!=len(self.pkey): raise ValueError('bad pkey')
      pkey_vals=map(row.__getitem__,indexes)
      return next((r for r in self.rows if pkey_vals==map(r.__getitem__,indexes)),None)
    else:
      # warning: is this right? it's saying that if not given, the pkey is the whole row. test dupe inserts on a real DB.
      return row if row in self.rows else None

  def expand_row(self, fields, values):
    "helper for insert. turn (field_names, values) into the full-width, properly-ordered row"
    table_fieldnames=[f.name for f in self.fields]
    reverse_indexes={table_fieldnames.index(f):i for i, f in enumerate(fields)}
    indexes=[reverse_indexes.get(i) for i in range(len(self.fields))]
    return [(Missing if i is None else values[i]) for i in indexes]

  def fix_rowtypes(self, row):
    if len(row)!=len(self.fields):
      raise ValueError('wrong # of values for table', self.name, self.fields, row)
    return map(toliteral, row)
  
  def apply_defaults(self, row, tables_dict):
    "apply defaults to missing cols for a row that's being inserted"
    return [
      emergency_cast(colx, field_default(colx, self.name, tables_dict) if v is Missing else v)
      for colx,v in zip(self.fields,row)
    ]
  
  def insert(self, fields, values, returning, tables_dict):
    print fields, values, returning
    raise NotImplementedError("this can't import sqex")
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    expanded_row=self.fix_rowtypes(self.expand_row(fields,values) if fields else values)
    row=self.apply_defaults(expanded_row, tables_dict)
    # todo: check ColX.not_null here. figure out what to do about null pkey field
    for i,elt in enumerate(row):
      raise NotImplementedError('port old Evaluator')
      row[i]=sqex.Evaluator(row,nix,tables_dict).eval(elt)
    if self.pkey_get(row): raise pg.DupeInsert(row)
    self.rows.append(row)
    if returning: return sqex.Evaluator((row,),nix,tables_dict).eval(returning)
  
  def match(self,where,tables,nix):
    raise NotImplementedError("this can't import sqex")
    raise NotImplementedError('is this used?')
    raise NotImplementedError('port old Evaluator')
    return [r for r in self.rows if not where or threevl.ThreeVL.test(sqex.Evaluator((r,),nix,tables).eval(where))]
  
  def lookup(self,name):
    if isinstance(name,sqparse2.NameX): name = name.name # this is horrible; be consistent
    try: return FieldLookup(*next((i,f) for i,f in enumerate(self.fields) if f.name==name))
    except StopIteration: raise BadFieldName(name)
  
  def update(self, rowlist):
    "replace rows from rowlist using indexes"
    raise NotImplementedError('todo')
    raise NotImplementedError("delete old code below")
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    if not all(isinstance(x,sqparse2.AssignX) for x in setx): raise TypeError('not_xassign',map(type,setx))
    match_rows=self.match(where,tables_dict,nix) if where else self.rows
    raise NotImplementedError('port old Evaluator')
    for row in match_rows:
      for x in setx: row[self.lookup(x.col).index]=sqex.Evaluator((row,),nix,tables_dict).eval(x.expr)
    if returning: return sqex.Evaluator((row,),nix,tables_dict).eval(returning)
  
  def delete(self, rowlist):
    "use indexes from rowlist to delete"
    raise NotImplementedError('todo')
    raise NotImplementedError("delete old code below")
    # todo: what's the deal with nested selects in delete. does it get evaluated once to a scalar before running the delete?
    # todo: this will crash with empty where clause
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    # todo(doc): why 'not' below?
    raise NotImplementedError('port old Evaluator')
    self.rows=[r for r in self.rows if not sqex.Evaluator((r,),nix,tables_dict).eval(where)]
