"weval -- where-clause evaluation"

import collections
from . import sqparse2, sqex, misc, scope, table, treepath

class RowType(list):
  "ctor takes list of (name, type). name is string, type is a sqparse2.ColX."
  def index(self, name):
    # todo: when a name isn't found, this should look in any children that have type=RowType
    return zip(*self)[0].index(name)

class RowSource:
  "for things like update and delete we need to know where a row came from. this stores that."
  def __init__(self, table, index):
    "table is a table.Table or a scope.SyntheticTable"
    self.table, self.index = table, index

class Row:
  def __init__(self, source, type, vals):
    "source is a RowSource or None if it isn't from a table"
    if len(type) != len(vals):
      raise ValueError('type/vals length mismatch', len(type), len(vals))
    self.source, self.type, self.vals = source, type, vals

  def __getitem__(self, name):
    return self.vals[self.type.index(name)]

SingleTableCond = collections.namedtuple('SingleTableCond', 'table exp')
CartesianCond = collections.namedtuple('CartesianCond', 'exp')

def names_from_exp(exp):
  "Return a list of AttrX and NameX from the expression."
  def match(exp):
    return isinstance(exp, (sqparse2.NameX, sqparse2.AttrX))
  paths = treepath.sub_slots(exp, match, match=True, recurse_into_matches=False)
  return [exp[path] for path in paths]

def classify_wherex(scope_, fromx, wherex):
  "helper for wherex_to_rowlist. returns [SingleTableCond,...], [CartesianCond,...]"
  exprs = []
  for exp in fromx:
    if isinstance(exp, sqparse2.JoinX):
      # todo: probably just add exp.on_stmt as a CartesianCond. don't write this until tests are ready.
      # todo: do join-on clauses get special scoping w.r.t. column names? check spec.
      raise NotImplementedError('join')
    elif isinstance(exp, str):
      exprs.append(exp)
  def test_and(exp):
    return isinstance(exp, sqparse2.BinX) and exp.op.op == 'and'
  def binx_splitter(exp):
    return [exp.left, exp.right]
  exprs += treepath.flatten_tree(test_and, binx_splitter, wherex) if wherex else [] # wherex is None if not given
  single_conds = []
  cartesian_conds = []
  for exp in exprs:
    if isinstance(exp, str):
      # note: bare table names need their own case because they don't work with resolve_column
      single_conds.append(SingleTableCond(exp, exp))
    else:
      tables = list(zip(*map(scope_.resolve_column, names_from_exp(exp))))[0]
      if len(tables) > 1:
        cartesian_conds.append(CartesianCond(exp))
      else:
        single_conds.append(SingleTableCond(tables[0], exp))
  return single_conds, cartesian_conds

def table_to_rowlist(table_, conds):
  "helper for wherex_to_rowlist. (table.Table, [exp, ...]) -> [Row, ...]"
  if isinstance(table_, scope.SyntheticTable):
    raise NotImplementedError('todo: synthetic tables to Row[]')
  elif isinstance(table_, table.Table):
    rowtype = RowType([(colx.name, colx) for colx in table_.fields])
    rows = [
      Row(RowSource(table_, i), rowtype, row)
      for i, row in enumerate(table_.rows)
    ]
    raise NotImplementedError # how does filtering work?
  else:
    raise TypeError('bad type for table', type(table), table)

def wherex_to_rowlist(scope_, fromx, wherex):
  """return a RowList with the rows included from scope by the fromx and wherex.
  When the scope has more than one name in it, the output will be a list of composite row
    (i.e. a row whose field types are themselves RowType).
  """
  single, multi = classify_wherex(scope_, fromx, wherex)
  single_rowlists = {
    tablename: table_to_rowlist(scope_[tablename], conds)
    for tablename, conds in list(misc.multimap(single).items()) # i.e. {cond.table:[cond.exp, ...]}
  }
  raise NotImplementedError
