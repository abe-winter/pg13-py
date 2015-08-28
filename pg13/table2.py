"table2 -- Table2 class for testing 'sliceable' behavior (restrict columns)"

from . import sqparse2

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

class Table:
  "this is for storage and also for managing intermediate results during queries"
  def __init__(self, names, expr=None, rows=(), alias=None):
    # note: list(rows) is both a cast and a copy (but each row is still a reference)
    self.names, self.expr, self.rows, self.alias = names, expr, list(rows), alias
    self.pkey = assemble_pkey(expr) if expr is not None else []

  @classmethod
  def from_create(class_, expr):
    "takes a CreateX, constructs a Table"
    if not isinstance(expr, sqparse2.CreateX):
      raise TypeError(type(expr), expr)
    if expr.inherits:
      raise TypeError("don't use Table.create for inherited tables", exp)
    names = [col.name for col in expr.cols]
    return class_(names, expr, alias=expr.name)
