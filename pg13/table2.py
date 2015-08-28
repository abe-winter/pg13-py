"table2 -- Table2 class for testing 'sliceable' behavior (restrict columns)"

from . import sqparse2

class Missing: "for distinguishing missing columns vs passed-in null"

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

def toliteral(probably_literal):
  # todo: among the exception cases are Missing, str. go through cases and make this cleaner. the test suite alone has multiple types here.
  if probably_literal==sqparse2.NameX('null'): return None
  return probably_literal.toliteral() if hasattr(probably_literal, 'toliteral') else probably_literal

def col2string(expr):
  "helper for the Table.fromx specializations. takes various expression types, returns a string (or None if not name-able)"
  if isinstance(expr, basestring): return expr
  elif isinstance(expr, sqparse2.NameX): return expr.name
  else: raise TypeError(expr)

class ColumnName:
  ""
  def __init__(self, name, tablename=None):
    raise NotImplementedError

  def match(self, expr):
    ""
    if isinstance(expr, sqparse2.NameX): raise NotImplementedError
    elif isinstance(expr, sqparse2.AliasX): raise NotImplementedError
    elif isinstance(expr, basestring): raise NotImplementedError
    else: raise TypeError(type(expr))

class Table(list):
  "this is for storage and also for managing intermediate results during queries"
  def __init__(self, names, expr=None, rows=(), alias=None):
    # note: list(rows) is both a cast and a copy (but each row is still a reference)
    # note: I *think* expr is mandatory for some commands (insert, for example) but optional in other cases (inside selects)
    self.names, self.expr, self.alias = names, expr, alias
    self.pkey = assemble_pkey(expr) if isinstance(expr, sqparse2.CreateX) else []
    super(Table, self).__init__(rows)

  @property
  def col_exprs(self):
    if isinstance(self.expr, sqparse2.CreateX): return self.expr.cols
    elif isinstance(self.expr, sqparse2.CommaX): raise NotImplementedError
    elif isinstance(self.expr, sqparse2.AsterX): raise NotImplementedError
    elif isinstance(self.expr, sqparse2.SelectX): raise NotImplementedError
    else:
      raise TypeError('unk expr type', self.expr)

  @classmethod
  def fromx(class_, expr):
    factory_fn = {
      sqparse2.CreateX: class_.from_create,
      sqparse2.CommaX: class_.from_commax,
    }[type(expr)]
    return factory_fn(expr)

  @classmethod
  def from_commax(class_, expr):
    "(Commax) -> Table"
    return class_(map(col2string, expr.children), expr)

  @classmethod
  def from_create(class_, expr):
    "takes a CreateX, constructs a Table"
    if not isinstance(expr, sqparse2.CreateX):
      raise TypeError(type(expr), expr)
    if expr.inherits:
      raise TypeError("don't use Table.create for inherited tables", exp)
    names = [col.name for col in expr.cols]
    return class_(names, expr, alias=expr.name)

  def expand_row(self, fields, values):
    "helper for insert. turn (field_names, values) into the full-width, properly-ordered row"
    reverse_indexes = {self.names.index(f):i for i, f in enumerate(fields)}
    indexes = [reverse_indexes.get(i) for i in range(len(self.names))]
    return [(Missing if i is None else values[i]) for i in indexes]

  def fix_rowtypes(self, row):
    "this should eventually do type-checking, maybe. for now it checks length and applies toliteral()"
    if len(row) != len(self.names):
      raise ValueError('wrong # of values for table', self.name, self.names, row)
    literals = map(toliteral, row)
    # todo: check types here
    return literals

  def pkey_get(self, row):
    """return the db row matched by the pkey values in the passed row.
    If this returns non-null an insert would fail (i.e. there's a dupe).
    """
    if len(row) != len(self.names):
      raise ValueError("bad row length", row, self.names)
    if self.pkey:
      indexes=[i for i, name in enumerate(self.names) if name in self.pkey]
      if len(indexes) != len(self.pkey):
        raise ValueError('pkey has unk fields', self.pkey, self.names)
      pkey_vals = [row[i] for i in indexes]
      return next((irow for irow in self if pkey_vals == [irow[i] for i in indexes]), None)
    else:
      return None

  def copy(self, rows):
    "return a copy of this table, replacing rows"
    return Table(self.names, self.expr, rows, self.alias)

  def get_field(self, name, row):
    "return field index from given row"
    return row[self.names.index(name)]
