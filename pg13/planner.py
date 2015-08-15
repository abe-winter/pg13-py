"planner -- extract predicates and joins from queries"

import collections, itertools
from . import sqparse2, sqex, misc, scope, table, treepath

SingleTableCond = collections.namedtuple('SingleTableCond', 'table exp')
CartesianCond = collections.namedtuple('CartesianCond', 'exp')

class OrderedSet(list):
  "this isn't efficient for long lists"
  def add(self, x):
    if x not in self:
      super(OrderedSet, self).append(x)
  def append(self, x): raise TypeError("no append on OrderedSet")
  def update(self, items): map(self.add, items)

def names_from_exp(exp):
  "Return a list of AttrX and NameX from the expression."
  def match(exp):
    return isinstance(exp, (sqparse2.NameX, sqparse2.AttrX))
  paths = treepath.sub_slots(exp, match, match=True, recurse_into_matches=False)
  return [exp[path] for path in paths]

def classify_wherex(scope_, fromx, wherex):
  "helper for wherex_to_rowlist. returns table_names_set, [SingleTableCond,...], [CartesianCond,...]"
  exprs = []
  for exp in fromx:
    if isinstance(exp, sqparse2.JoinX):
      # todo: do join-on clauses get special scoping w.r.t. column names? check spec.
      exprs.append(exp.on_stmt)
    elif isinstance(exp, basestring): exprs.append(exp)
    elif isinstance(exp, sqparse2.AliasX): exprs.append(exp.alias) # i.e. treat it like a table name
    else: raise TypeError('unk type in fromx', type(exp), exp)
  def test_and(exp):
    return isinstance(exp, sqparse2.BinX) and exp.op.op == 'and'
  def binx_splitter(exp):
    return [exp.left, exp.right]
  exprs += treepath.flatten_tree(test_and, binx_splitter, wherex) if wherex else [] # wherex is None if not given
  single_conds = []
  cartesian_conds = []
  table_names = OrderedSet()
  for exp in exprs:
    if isinstance(exp, basestring): table_names.add(exp) # but don't store to either conds list
    else:
      tables = zip(*map(scope_.resolve_column, names_from_exp(exp)))[0]
      table_names.update(tables)
      if len(tables) > 1:
        cartesian_conds.append(CartesianCond(exp))
      else:
        single_conds.append(SingleTableCond(tables[0], exp))
  return table_names, single_conds, cartesian_conds

def table_to_rowlist(table_, alias):
  "helper for wherex_to_rowlist. (table.Table, [exp, ...]) -> [Row, ...]"
  # why no call to_rowlist directly? there may be another case on the way (SelectResult)
  if isinstance(table_, table.Table):
    return table_.to_rowlist(alias)
  else:
    raise TypeError('bad type for table', type(table), table)

def conds_on_row(scope_, row, conds):
  evaluator = sqex.Evaluator2(row, scope_)
  return all(evaluator.eval(exp) for exp in conds)

def filter_rowlist(scope_, rowlist, conds):
  # todo: an analyzer can put the cheapest cond first
  return table.RowList([
    row for row in rowlist
    if conds_on_row(scope_, row, conds)
  ])

def make_composite_rows(rowlists):
  "take list of lists of Row. return generator of Row with source=Composite by creating the product of the lists"
  for i, rows in enumerate(itertools.product(*rowlists)):
    yield table.Row(table.RowSource(None, table.Composite, i), rows)

class Plan:
  ""
  @classmethod
  def from_query(class_, scope_, fromx, wherex):
    return class_(*classify_wherex(scope_, fromx, wherex))

  def __init__(self, table_names, predicates, joins):
    self.table_names, self.predicates, self.joins = table_names, predicates, joins

  def run(self, scope_):
    """return a rowlist with the rows included from scope by the fromx and wherex.
    When the scope has more than one name in it, the output will be a list of composite row
      (i.e. a row whose field types are themselves RowType).
    """
    print 'run scope', scope_
    single_rowlists = {
      table_name: filter_rowlist(scope_, table_to_rowlist(scope_[table_name], table_name), conds)
      for table_name, conds in misc.multimap(self.predicates).items() # i.e. {cond.table:[cond.exp, ...]}
    }
    rowlists = {
      # this adds in the tables that are referenced in the fromx but have no predicate
      table_name: single_rowlists[table_name] if table_name in single_rowlists else table_to_rowlist(scope_[table_name], table_name)
      for table_name in self.table_names
    }
    if len(rowlists) == 1 and not self.joins:
      return table.RowList(rowlists.values()[0])
    else:
      if len(self.table_names) != len(rowlists):
        raise RuntimeError("expected table_names and rowlists keys to match", self.table_names, list(rowlists))
      return filter_rowlist(
        scope_,
        make_composite_rows([rowlists[name] for name in self.table_names]),
        [cond.exp for cond in self.joins]
      )

  def explain(self): raise NotImplementedError('todo')
  def __repr__(self): return '<Plan %s %i predicates %i joins>' % (self.table_names, len(self.predicates), len(self.joins))
