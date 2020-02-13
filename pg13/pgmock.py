"table class and apply_sql. this is weirdly codependent with sqex.py"

# todo: type checking of literals based on column. flag-based (i.e. not all DBs do this) cast strings to unicode.

import re,collections,contextlib,threading,copy
from . import pg,threevl,sqparse2,sqex,table

class TablesDict:
  "dictionary wrapper that knows about transactions"
  # todo: bite the bullet and rename this Database
  def __init__(self):
    self.lock = threading.Lock()
    self.levels = [{}]
    self.transaction = False
    self.transaction_owner = None
  
  def __getitem__(self, k): return self.levels[-1][k]
  def __setitem__(self, k, v): self.levels[-1][k] = v
  def __contains__(self, k): return k in self.levels[-1]
  def __delitem__(self, k): del self.levels[-1][k]
  def update(self, *args, **kwargs): self.levels[-1].update(*args,**kwargs)
  def __iter__(self): return iter(self.levels[-1])
  def keys(self): return list(self.levels[-1].keys())
  def values(self): return list(self.levels[-1].values())
  def items(self): return list(self.levels[-1].items())
  
  @contextlib.contextmanager
  def tempkeys(self):
    """Add a new level to make new keys temporary. Used instead of copy in sqex.
    This may *seem* similar to a transaction but the tables are not being duplicated, just referenced.
    At __exit__, old dict is restored (but changes to Tables remain).
    """
    self.levels.append(dict(self.levels[-1]))
    try: yield
    finally: self.levels.pop()
  
  def trans_start(self, lockref):
    self.lock.acquire()
    if self.transaction: raise RuntimeError('in transaction after acquiring lock')
    self.levels.append(copy.deepcopy(self.levels[0])) # i.e. copy all the tables, too
    self.transaction = True
    self.transaction_owner = lockref
  
  def trans_commit(self):
    if not self.transaction: raise RuntimeError('commit not in transaction')
    self.levels = [self.levels[1]]
    self.transaction = False
    self.lock.release()
  
  def trans_rollback(self):
    if not self.transaction: raise RuntimeError('commit not in transaction')
    self.levels = [self.levels[0]]
    self.transaction = False
    self.lock.release()
  
  @contextlib.contextmanager
  def lock_db(self,lockref,is_start):
    if self.transaction and self.transaction_owner is lockref:
      # note: this case is relying on the fact that if the above is true, our thread did it,
      #   therefore the lock can't be released on our watch.
      yield
    elif is_start: yield # apply_sql will call trans_start() on its own, block there if necessary
    else:
      with self.lock: yield
  
  def cascade_delete(self, name):
    "this fails under diamond inheritance"
    for child in self[name].child_tables:
      self.cascade_delete(child.name)
    del self[name]
  
  def create(self, ex):
    "helper for apply_sql in CreateX case"
    if ex.name in self:
      if ex.nexists: return
      raise ValueError('table_exists',ex.name)
    if any(c.pkey for c in ex.cols):
      if ex.pkey:
        raise sqparse2.SQLSyntaxError("don't mix table-level and column-level pkeys",ex)
      # todo(spec): is multi pkey permitted when defined per column?
      ex.pkey = sqparse2.PKeyX([c.name for c in ex.cols if c.pkey])
    if ex.inherits:
      # todo: what if child table specifies constraints etc? this needs work.
      if len(ex.inherits) > 1: raise NotImplementedError('todo: multi-table inherit')
      parent = self[ex.inherits[0]] = copy.deepcopy(self[ex.inherits[0]]) # copy so rollback works
      child = self[ex.name] = table.Table(ex.name, parent.fields, parent.pkey)
      parent.child_tables.append(child)
      child.parent_table = parent
    else:
      self[ex.name]=table.Table(ex.name,ex.cols,ex.pkey.fields if ex.pkey else [])
  
  def drop(self, ex):
    "helper for apply_sql in DropX case"
    # todo: factor out inheritance logic (for readability)
    if ex.name not in self:
      if ex.ifexists: return
      raise KeyError(ex.name)
    table_ = self[ex.name]
    parent = table_.parent_table
    if table_.child_tables:
      if not ex.cascade:
        raise table.IntegrityError('delete_parent_without_cascade',ex.name)
      self.cascade_delete(ex.name)
    else: del self[ex.name]
    if parent: parent.child_tables.remove(table_)
  
  def apply_sql(self, ex, values, lockref):
    """call the stmt in tree with values subbed on the tables in t_d.
    ex is a parsed statement returned by parse_expression.
    values is the tuple of %s replacements.
    lockref can be anything as long as it stays the same; it's used for assigning tranaction ownership.
      (safest is to make it a pgmock_dbapi2.Connection, because that will rollback on close)
    """
    sqex.depth_first_sub(ex,values)
    with self.lock_db(lockref, isinstance(ex,sqparse2.StartX)):
      sqex.replace_subqueries(ex,self,table.Table)
      if isinstance(ex,sqparse2.SelectX): return sqex.run_select(ex,self,table.Table)
      elif isinstance(ex,sqparse2.InsertX): return self[ex.table].insert(ex.cols,ex.values,ex.ret,self)
      elif isinstance(ex,sqparse2.UpdateX):
        if len(ex.tables)!=1: raise NotImplementedError('multi-table update')
        return self[ex.tables[0]].update(ex.assigns,ex.where,ex.ret,self)
      elif isinstance(ex,sqparse2.CreateX): self.create(ex)
      elif isinstance(ex,sqparse2.IndexX): pass
      elif isinstance(ex,sqparse2.DeleteX): return self[ex.table].delete(ex.where,self)
      elif isinstance(ex,sqparse2.StartX): self.trans_start(lockref)
      elif isinstance(ex,sqparse2.CommitX): self.trans_commit()
      elif isinstance(ex,sqparse2.RollbackX): self.trans_rollback()
      elif isinstance(ex,sqparse2.DropX): self.drop(ex)
      else: raise TypeError(type(ex)) # pragma: no cover
