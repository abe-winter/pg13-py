"weval -- where-clause evaluation"

import collections

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

def classify_wherex(wherex):
  "helper for wherex_to_rowlist. returns [SingleTableCond,...], [CartesianCond,...]"
  single_conds = []
  cartesian_conds = []
  raise NotImplementedError

def wherex_to_rowlist(scope, wherex):
  """return a RowList with the rows included from scope by the wherex.
  When the scope has more than one name in it, the output will be a list of composite row
    (i.e. a row whose field types are themselves RowType).
  """
  raise NotImplementedError
