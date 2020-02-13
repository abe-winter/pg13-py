"scope -- storage class for managing an expression's tables and aliases"

from . import sqparse2, table

class ScopeError(Exception): "base"
class ScopeCollisionError(ScopeError): pass
class ScopeUnkError(ScopeError): pass

def col2name(col_item):
  "helper for SyntheticTable.columns. takes something from SelectX.cols, returns a string column name"
  if isinstance(col_item, sqparse2.NameX): return col_item.name
  elif isinstance(col_item, sqparse2.AliasX): return col_item.alias
  else: raise TypeError(type(col_item), col_item)

class SyntheticTable:
  def __init__(self, exp):
    if not isinstance(exp, sqparse2.SelectX):
      raise TypeError('expected SelectX for', type(exp), exp)
    self.exp = exp

  def columns(self, scope):
    "return list of column names. needs scope for resolving asterisks."
    return list(map(col2name, self.exp.cols.children))

class Scope:
  "bundle for all the tables that are going to be used in a query, and their aliases"
  def __init__(self, expression):
    self.expression = expression
    self.names = {}

  def __contains__(self, name):
    return name in self.names

  def add(self, name, target):
    "target should be a Table or SyntheticTable"
    if not isinstance(target, (table.Table, SyntheticTable)):
      raise TypeError(type(target), target)
    if name in self:
      # note: this is critical for avoiding cycles
      raise ScopeCollisionError('scope already has', name)
    self.names[name] = target

  def __getitem__(self, table_name):
    return self.names[table_name]

  def resolve_column(self, ref):
    "ref is a NameX or AttrX. return (canonical_table_name, column_name)."
    if isinstance(ref, sqparse2.AttrX):
      if ref.parent.name not in self:
        raise ScopeUnkError('unk table or table alias', ref.parent.name)
      return ref.parent.name, ref.attr.name
    elif isinstance(ref, sqparse2.NameX):
      matches = set()
      for name, target in list(self.names.items()):
        if isinstance(target, SyntheticTable):
          if ref.name in target.columns(self):
            matches.add(name)
        elif isinstance(target, table.Table):
          try: target.get_column(ref.name)
          except KeyError: pass
          else: matches.add(name)
        else:
          raise TypeError('expected SyntheticTable', type(target), target)
      if not matches: raise ScopeUnkError(ref)
      elif len(matches) > 1: raise ScopeCollisionError(matches, ref)
      else: return list(matches)[0], ref.name
    else:
      raise TypeError('unexpected', type(ref), ref)

  @classmethod
  def from_fromx(class_, tables, fromx, ctes=()):
    """Build a Scope given TablesDict, from-expression and optional list of CTEs.
    fromx is a list of expressions (e.g. SelectX.tables). The list elts can be:
      1. string (i.e. tablename)
      2. AliasX(SelectX as name)
      3. AliasX(name as name)
    """
    if ctes: raise NotImplementedError # note: I don't think any other part of the program supports CTEs yet either
    scope_ = class_(fromx)
    for exp in fromx:
      if isinstance(exp, str):
        scope_.add(exp, tables[exp])
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, sqparse2.NameX):
        scope_.add(exp.alias, tables[exp.name.name])
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, sqparse2.SelectX):
        scope_.add(exp.alias, SyntheticTable(exp.name))
      elif isinstance(exp, sqparse2.JoinX):
        raise NotImplementedError('todo: join')
      else:
        raise TypeError('bad fromx type', type(exp), exp)
    return scope_
