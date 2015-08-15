"table -- Table & Row storage classes. operations on tables are in commands.py"

import collections
from . import threevl, sqparse2

# errors
class PgExecError(sqparse2.PgMockError): "base class for errors during table execution"
class BadFieldName(PgExecError): pass
class IntegrityError(PgExecError): pass # careful: pgmock_dbapi also defines this
class UnkTableError(StandardError): "for lookup failures in composite rows"

class Missing: "for distinguishing missing columns vs passed-in null"

FieldLookup=collections.namedtuple('FieldLookup','index type')

class Composite: "use this for RowSource.table when a Row is composite"

class RowList(list): "subclass of list; identical except for type-checking"

class RowSource:
  "for things like update and delete we need to know where a row came from. this stores that."
  def __init__(self, table, index):
    "table is a table.Table or a scope.SyntheticTable. index an integer or None (None for rows about to be inserted)"
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

  @classmethod
  def construct(class_, table_, index, vals):
    if not isinstance(table_, Table): raise TypeError(type(table_), table_)
    return class_(RowSource(table_, index), vals)

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

  @property
  def allvals(self):
    "returns ordered sub-row vals from a composite row (or self.vals if not composite)"
    # note: casting to list so uoter sum works
    return list(sum((row.allvals for row in self.vals),[]) if self.source.table is Composite else self.vals)

  def __getitem__(self, (table_name, column_name)):
    actual_row = self.get_table(table_name)
    if not actual_row:
      raise UnkTableError(table_name, self)
    return actual_row.vals[actual_row.index(column_name)]

  def __repr__(self): return '<Row %s:%s %r>' % (self.source.name or 'composite', self.source.index, self.vals)

def toliteral(probably_literal):
  # todo: among the exception cases are Missing, str. go through cases and make this cleaner. the test suite alone has multiple types here.
  if probably_literal==sqparse2.NameX('null'): return None
  return probably_literal.toliteral() if hasattr(probably_literal, 'toliteral') else probably_literal

def assemble_pkey(exp):
  "helper for Table.create. returns the pkey fields if given directly, otherwise constructs one from columns."
  if not isinstance(exp, sqparse2.CreateX):
    raise TypeError(type(exp), exp)
  if any(c.pkey for c in exp.cols):
    if exp.pkey:
      raise sqparse2.SQLSyntaxError("don't mix table-level and column-level pkeys", exp)
    # todo(spec): is multi pkey permitted when defined per column?
    return [c.name for c in exp.cols if c.pkey]
  else:
    return exp.pkey.fields if exp.pkey else []

class Table:
  def __init__(self, name, fields, pkey):
    "fields is a list of sqparse2.ColX"
    self.name,self.fields,self.pkey=name,fields,(pkey or [])
    self.rows=[]
    self.child_tables=[] # tables that inherit from this one
    self.parent_table=None # table this inherits from
  
  @classmethod
  def create(class_, exp):
    "takes a CreateX, constructs a Table"
    if not isinstance(exp, sqparse2.CreateX):
      raise TypeError(type(exp), exp)
    if exp.inherits:
      raise TypeError("don't use Table.create for inherited tables", exp)
    return Table(exp.name, exp.cols, assemble_pkey(exp))

  def to_rowlist(self):
    "return [Row, ...] for intermediate computations"
    return [Row(RowSource(self, i), row) for i, row in enumerate(self.rows)]

  def get_column(self, name):
    col = next((f for f in self.fields if f.name==name), None)
    if col is None: raise KeyError(name)
    return col

  def expand_row(self, fields, values):
    "helper for insert. turn (field_names, values) into the full-width, properly-ordered row"
    table_fieldnames=[f.name for f in self.fields]
    reverse_indexes={table_fieldnames.index(f):i for i, f in enumerate(fields)}
    indexes=[reverse_indexes.get(i) for i in range(len(self.fields))]
    return [(Missing if i is None else values[i]) for i in indexes]
  
  def lookup(self, name):
    if isinstance(name,sqparse2.NameX): name = name.name # this is horrible; be consistent
    try: return FieldLookup(*next((i,f) for i,f in enumerate(self.fields) if f.name==name))
    except StopIteration: raise BadFieldName(name)

  def fix_rowtypes(self, row):
    "this should eventually do type-checking, maybe. for now it checks length and applies toliteral()"
    if len(row) != len(self.fields):
      raise ValueError('wrong # of values for table', self.name, self.fields, row)
    literals = map(toliteral, row)
    # todo: check types here
    return literals

  def pkey_get(self, row):
    """return the db row matched by the pkey values in the passed row.
    If this returns non-null an insert would fail (i.e. there's a dupe).
    """
    if len(row) != len(self.fields):
      raise ValueError("bad row length", row, self.fields)
    if self.pkey:
      indexes=[i for i,f in enumerate(self.fields) if f.name in self.pkey]
      if len(indexes) != len(self.pkey):
        raise ValueError('pkey has unk fields', self.pkey, self.fields)
      pkey_vals = map(row.__getitem__,indexes) 
      return next((r for r in self.rows if pkey_vals==map(r.__getitem__,indexes)), None)
    else:
      return None
