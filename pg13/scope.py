"scope -- storage class for managing an expression's tables and aliases"

from . import sqparse2, table2, treepath

class ScopeError(StandardError): "base"
class ScopeCollisionError(ScopeError): pass
class ScopeUnkError(ScopeError): pass

SUBTABLE_TYPES = (sqparse2.SelectX, sqparse2.CommaX, sqparse2.AsterX)

class Scope(dict):
  "bundle for all the tables that are going to be used in a query, and their aliases"
  def __init__(self, expression):
    self.expression = expression
    super(Scope, self).__init__()

  def add(self, name, target):
    "target should be a Table"
    if not isinstance(target, table2.Table):
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
        if ref.name in target.names:
          matches.add(name)
      if not matches: raise ScopeUnkError(ref)
      elif len(matches) > 1: raise ScopeCollisionError(matches, ref)
      else: return list(matches)[0], ref.name
    else:
      raise TypeError('unexpected', type(ref), ref)

  def replace_intermediate_types(self, expr):
    """Replace CommaX/SelectX/AsterX with Table so we can track the names of variables.
    (doesn't apply to top-level SelectX, only subqueries. todo: think more about scalar subqueries when I know more).
    This is potentially useful at the output stage but critical with subqueries.
    Example: select a from (select * from t1) as whatever;
    """
    print expr
    def test(expr):
      return isinstance(expr, SUBTABLE_TYPES)
    for path in treepath.sub_slots(expr, test):
      # warning: is the next line valid with len(path)=1?
      replace_path = path[:-1] if isinstance(expr[path[:-1]], sqparse2.ReturnX) else path
      expr[replace_path] = table2.Table.fromx(expr[path])

  @classmethod
  def from_expr(class_, tables, expr):
    if isinstance(expr, sqparse2.SelectX):
      return class_.from_fromx(tables, expr.tables)
    elif isinstance(expr, sqparse2.InsertX):
      return class_.from_fromx(tables, [expr.table])
    elif isinstance(expr, sqparse2.UpdateX): raise NotImplementedError
    elif isinstance(expr, sqparse2.DeleteX): raise NotImplementedError
    elif isinstance(expr, sqparse2.CreateX): return class_([])
    else:
      raise NotImplementedError(type(expr))

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
      elif isinstance(exp, sqparse2.JoinX):
        scope_.add(exp.a, tables[exp.a])
        scope_.add(exp.b, tables[exp.b])
      else:
        raise TypeError('bad fromx type', type(exp), exp)
    return scope_
