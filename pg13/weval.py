"weval -- where-clause evaluation"

import collections
from . import sqparse2, sqex, misc, scope, table, treepath

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
    elif isinstance(exp, basestring):
      exprs.append(exp)
  def test_and(exp):
    return isinstance(exp, sqparse2.BinX) and exp.op.op == 'and'
  def binx_splitter(exp):
    return [exp.left, exp.right]
  exprs += treepath.flatten_tree(test_and, binx_splitter, wherex) if wherex else [] # wherex is None if not given
  single_conds = []
  cartesian_conds = []
  for exp in exprs:
    if isinstance(exp, basestring):
      # note: bare table names need their own case because they don't work with resolve_column
      single_conds.append(SingleTableCond(exp, exp))
    else:
      tables = zip(*map(scope_.resolve_column, names_from_exp(exp)))[0]
      if len(tables) > 1:
        cartesian_conds.append(CartesianCond(exp))
      else:
        single_conds.append(SingleTableCond(tables[0], exp))
  return single_conds, cartesian_conds

def table_to_rowlist(table_):
  "helper for wherex_to_rowlist. (table.Table, [exp, ...]) -> [Row, ...]"
  if isinstance(table_, scope.SyntheticTable):
    raise NotImplementedError('todo: synthetic tables to Row[]')
  elif isinstance(table_, table.Table): return table_.to_rowlist()
  else:
    raise TypeError('bad type for table', type(table), table)

def conds_on_row(scope_, row, conds):
  evaluator = sqex.Evaluator2(row, scope_)
  return all(evaluator.eval(exp) for exp in conds)

def filter_rowlist(scope_, rowlist, conds):
  # todo: an analyzer can put the cheapest cond first
  return [
    row for row in rowlist
    if conds_on_row(scope_, row, conds)
  ]

def wherex_to_rowlist(scope_, fromx, wherex):
  """return a rowlist with the rows included from scope by the fromx and wherex.
  When the scope has more than one name in it, the output will be a list of composite row
    (i.e. a row whose field types are themselves RowType).
  """
  single, multi = classify_wherex(scope_, fromx, wherex)
  # note: we filter single before multi for performance -- the product of the tables is smaller if we can reduce the inputs
  single_rowlists = {
    tablename: filter_rowlist(scope_, table_to_rowlist(scope_[tablename]), conds)
    for tablename, conds in misc.multimap(single).items() # i.e. {cond.table:[cond.exp, ...]}
  }
  if len(single_rowlists) == 1 and not multi:
    return single_rowlists.values()[0]
  raise NotImplementedError('build composite_rows, apply composite conditions, return filtered composite_rows')
