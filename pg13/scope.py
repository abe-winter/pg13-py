"scope -- storage class for managing an expression's tables and aliases"

from . import sqparse2, table

class ScopeError(StandardError): "base"
class ScopeCollisionError(ScopeError): pass
class ScopeUnkError(ScopeError): pass

class Scope(dict):
  "bundle for all the tables that are going to be used in a query, and their aliases"
  def __init__(self, expression):
    self.expression = expression
    super(Scope, self).__init__()

  def add(self, name, target):
    "target should be a Table"
    if not isinstance(target, (table.Table, table.SelectResult)):
      raise TypeError(type(target), target)
    if name in self:
      # note: this is critical for avoiding cycles
      raise ScopeCollisionError('scope already has', name)
    self[name] = target

  def resolve_column(self, ref):
    "ref is a NameX or AttrX. return (canonical_table_name, column_name)."
    if isinstance(ref, sqparse2.AttrX):
      if ref.parent.name not in self:
        raise ScopeUnkError('unk table or table alias', ref.parent.name)
      return ref.parent.name, ref.attr.name
    elif isinstance(ref, sqparse2.NameX):
      matches = set()
      for name, target in self.items():
        try: target.get_column(ref.name)
        except KeyError: pass
        else: matches.add(name)
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
      if isinstance(exp, basestring):
        scope_.add(exp, tables[exp])
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, sqparse2.NameX):
        scope_.add(exp.alias, tables[exp.name.name])
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, sqparse2.SelectX):
        raise RuntimeError('subqueries should have been replaced with rowlist before this')
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, table.SelectResult):
        scope_.add(exp.alias, exp.name)
      elif isinstance(exp, sqparse2.JoinX):
        scope_.add(exp.a, tables[exp.a])
        scope_.add(exp.b, tables[exp.b])
      else:
        raise TypeError('bad fromx type', type(exp), exp)
    return scope_
