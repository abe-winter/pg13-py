"weval -- where-clause evaluation"

import collections
from . import sqparse2, sqex

class RowType(list):
  "ctor takes list of (name, type)"
  def index(self, name):
    # todo: when a name isn't found, this should look in any children that have type=RowType
    return zip(*self)[0].index(name)

class RowSource:
  "for things like update and delete we need to know where a row came from. this stores that."
  def __init__(self, table, index):
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
CartesianCond = collections.namedtuple('MultiTableCond', 'exp')

def flatten_tree(test, enumerator, exp):
  """test is function(exp) >> bool.
  mapper is function(expression) >> list of subexpressions.
  returns [subexpression, ...].
  """
  return sum((flatten_tree(test, enumerator, subx) for subx in enumerator(exp)), []) if test(exp) else [exp]

def names_from_exp(exp):
  "Return all the AttrX and NameX (i.e. variable names) from the expression."
  def match(exp):
    return isinstance(exp, (sqparse2.NameX, sqparse2.AttrX))
  paths = sqex.sub_slots(exp, match, match=True, recurse_into_matches=False)
  return [exp[path] for path in paths]

def classify_wherex(scope, fromx, wherex):
  "helper for wherex_to_rowlist. returns [SingleTableCond,...], [CartesianCond,...]"
  exprs = []
  for exp in fromx:
    if isinstance(exp, sqparse2.JoinX):
      # exp.on_stmt
      raise NotImplementedError('join')
  def test(exp):
    return isinstance(exp, sqparse2.BinX) and exp.op.op == 'and'
  def enumerator(exp):
    return [exp.left, exp.right]
  exprs += flatten_tree(test, enumerator, wherex)
  for exp in exprs:
    names = names_from_exp(exp)
    # scope. ...
  print 'exprs', exprs
  raise NotImplementedError('return single / cart split')

def wherex_to_rowlist(scope, fromx, wherex):
  """return a RowList with the rows included from scope by the wherex.
  fromx is used to determine join conditions.
  When the scope has more than one name in it, the output will be a list of composite row
    (i.e. a row whose field types are themselves RowType).
  """
  raise NotImplementedError
