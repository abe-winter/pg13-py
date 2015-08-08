"scope -- storage class for managing an expression's tables and aliases"

from . import sqparse2

class WevalError(StandardError): "base"
class ScopeCollisionError(WevalError): pass
class ScopeUnkError(WevalError): pass

class Lazy:
  "wrapper for expressions we want to evaluate as-needed"
  def __init__(self, action, args=(), kwargs={}):
    self.args, self.kwargs, self.action = args, kwargs, action
    self.computed, self.output = False, None

  @property
  def val(self):
    if not self.computed:
      self.output = self.action(*self.args, **self.kwargs)
      self.computed = True
    return self.output

def lazy_subselect(scope, exp):
  raise NotImplementedError

class Scope:
  "bundle for all the tables that are going to be used in a query, and their aliases"
  def __init__(self, expression, tables, parent=None):
    self.tables, self.expression, self.parent = tables, expression, parent
    self.aliases = {} # names that map to other names
    self.objects = {} # names that map to objects
    self.children = [] # sub-scopes; this probably will go away

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

  def get(self, name):
    """Get name from scope, walking up parents if necessary, preferring local to global.
    Resolve column names from tables and fail when columns are ambiguous.
    If the target is an instance of Lazy, this returns target.val.
    """
    # if this is going to return a value it needs to take a row as well
    # >> instead, maybe return (canonical_table, column)
    if name not in self:
      raise ScopeUnkError('Scope.get unk', name)
    raise NotImplementedError

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
      if isinstance(exp, basestring): pass
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, basestring):
        raise NotImplementedError
      elif isinstance(exp, sqparse2.AliasX) and isinstance(exp.name, sqparse2.SelectX):
        scope.add_object(exp.alias, Lazy(lazy_subselect, (scope, exp.name)))
      elif isinstance(exp, sqparse2.JoinX):
        raise NotImplementedError('todo: join')
      else:
        raise TypeError('bad fromx type', type(exp), exp)
    return scope
