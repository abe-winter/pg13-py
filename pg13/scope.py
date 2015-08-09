"scope -- storage class for managing an expression's tables and aliases"

from . import sqparse2

class ScopeError(StandardError): "base"
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
    return map(col2name, self.exp.cols.children)

class Scope:
  "bundle for all the tables that are going to be used in a query, and their aliases"
  def __init__(self, expression, tables, parent=None):
    self.tables, self.expression, self.parent = tables, expression, parent
    self.aliases = {} # names that map to other names
    self.objects = {} # names that map to objects -- I think this is always SyntheticTable

  def __contains__(self, name):
    return (name in self.tables) or (name in self.aliases) or (name in self.objects)

  def add_alias(self, alias, target):
    if alias in self:
      # note: this is critical for avoiding cycles
      raise ScopeCollisionError('scope already has', alias)
    # note: we don't check that target exists
    raise NotImplementedError

  def add_object(self, alias, object):
    if alias in self:
      # note: this is critical for avoiding cycles
      raise ScopeCollisionError('scope already has', alias)
    self.objects[alias] = object

  def get_table(self, name):
    raise NotImplementedError

  def resolve_column(self, ref):
    "ref is a NameX or AttrX. return (canonical_table_name, column_name)."
    if isinstance(ref, sqparse2.AttrX):
      raise NotImplementedError
    elif isinstance(ref, sqparse2.NameX):
      matches = set()
      for key, val in self.objects.items():
        if not isinstance(val, SyntheticTable):
          raise TypeError('expected SyntheticTable', type(val), val)
        if ref.name in val.columns(self):
          matches.add(key)
      for key, table in self.tables.items():
        try: table.get_column(ref.name)
        except: pass
        else: matches.add(key)
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
    scope = class_(fromx, tables)
    for exp in fromx:
      if isinstance(exp, basestring):
        print 'warning: I should be adding named tables instead of uatomatically reading from scope.tables. scope.tables might not be necessary'
        pass
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, basestring):
        raise NotImplementedError
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, sqparse2.SelectX):
        scope.add_object(exp.alias, SyntheticTable(exp.name))
      elif isinstance(exp, sqparse2.JoinX):
        raise NotImplementedError('todo: join')
      else:
        raise TypeError('bad fromx type', type(exp), exp)
    return scope
