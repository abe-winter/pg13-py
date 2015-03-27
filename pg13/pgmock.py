"table class and apply_sql. this is weirdly codependent with sqex.py"

# todo: type checking of literals based on column. flag-based (i.e. not all DBs do this) cast strings to unicode.

import re,collections,contextlib,threading,copy
from . import pg,threevl,sqparse2,sqex

# errors
class PgExecError(sqparse2.PgMockError): "base class for errors during table execution"
class BadFieldName(PgExecError): pass

class Missing: "for distinguishing missing columns vs passed-in null"

class TablesDict:
  "dictionary wrapper that knows about transactions"
  # todo: consider setting transaction_owner by cursor *and* thread
  # warning: lots of disaster can result from sharing cursors between threads
  def __init__(self):
    self.lock = threading.Lock()
    self.levels = [{}]
    self.transaction = False
    self.connection_owner = None
  def __getitem__(self, k): return self.levels[-1][k]
  def __setitem__(self, k, v): self.levels[-1][k] = v
  def __contains__(self, k): return k in self.levels[-1]
  def update(self, *args, **kwargs): self.levels[-1].update(*args,**kwargs)
  def __iter__(self): return iter(self.levels[-1])
  def keys(self): return self.levels[-1].keys()
  def values(self): return self.levels[-1].values()
  @contextlib.contextmanager
  def tempkeys(self):
    """Add a new level to make new keys temporary. Used instead of copy in sqex.
    This may *seem* similar to a transaction but the tables are not being duplicated, just referenced.
    At __exit__, old dict is restored (but changes to Tables remain).
    """
    self.levels.append(dict(self.levels[-1]))
    try: yield
    finally: self.levels.pop()
  def trans_start(self, cursor):
    self.lock.acquire()
    if self.transaction: raise RuntimeError('in transaction after acquiring lock')
    self.levels.append(copy.deepcopy(self.levels[0])) # i.e. copy all the tables, too
    self.transaction = True
    self.connection_owner = cursor
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
  def lock_db(self,cursor,is_start):
    if self.transaction and self.connection_owner is cursor:
      # note: this case is relying on the fact that if the above is true, our thread did it,
      #   therefore the lock can't be released on our watch.
      yield
    elif is_start: yield # apply_sql will call trans_start() on its own, block there if necessary
    else:
      with self.lock: yield

def expand_row(table_fields,fields,values):
  "helper for insert. turn (field_names, values) into the full-width, properly-ordered row"
  table_fieldnames=[f.name for f in table_fields]
  reverse_indexes={table_fieldnames.index(f):i for i,f in enumerate(fields)}
  indexes=[reverse_indexes.get(i) for i in range(len(table_fields))]
  return [(Missing if i is None else values[i]) for i in indexes]

def emergency_cast(colx, value):
  """ugly: this is a huge hack. get serious about where this belongs in the architecture.
  For now, most types rely on being fed in as SubbedLiteral.
  """
  if colx.coltp.lower()=='boolean':
    if isinstance(value,sqparse2.NameX): value = value.name
    if isinstance(value,bool): return value
    return dict(true=True, false=False)[value.lower()] # keyerror if other
  else:
    return value # todo: type check?

def field_default(colx, table_name, tables_dict):
  "takes sqparse2.ColX, Table"
  if colx.coltp.lower() == 'serial':
    x = sqparse2.parse('select coalesce(max(%s),-1)+1 from %s' % (colx.name, table_name))
    return sqex.run_select(x, tables_dict, Table)[0]
  elif colx.not_null: raise NotImplementedError('todo: not_null error')
  else: return toliteral(colx.default)

FieldLookup=collections.namedtuple('FieldLookup','index type')
def toliteral(probably_literal):
  # todo: among the exception cases are Missing, str. go through cases and make this cleaner. the test suite alone has multiple types here.
  if probably_literal==sqparse2.NameX('null'): return None
  return probably_literal.toliteral() if hasattr(probably_literal,'toliteral') else probably_literal
class Table:
  def __init__(self,name,fields,pkey):
    self.name,self.fields,self.pkey=name,fields,(pkey or [])
    self.rows=[]
  def pkey_get(self,row):
    if len(self.pkey):
      indexes=[i for i,f in enumerate(self.fields) if f.name in self.pkey]
      if len(indexes)!=len(self.pkey): raise ValueError('bad pkey')
      pkey_vals=map(row.__getitem__,indexes)
      return next((r for r in self.rows if pkey_vals==map(r.__getitem__,indexes)),None)
    else:
      # warning: is this right? it's saying that if not given, the pkey is the whole row. test dupe inserts on a real DB.
      return row if row in self.rows else None
  def fix_rowtypes(self,row):
    if len(row)!=len(self.fields): raise ValueError
    return map(toliteral,row)
  def apply_defaults(self, row, tables_dict):
    "apply defaults to missing cols for a row that's being inserted"
    return [emergency_cast(colx, field_default(colx, self.name, tables_dict) if v is Missing else v) for colx,v in zip(self.fields,row)]
  def insert(self,fields,values,returning,tables_dict):
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    expanded_row=self.fix_rowtypes(expand_row(self.fields,fields,values) if fields else values)
    row=self.apply_defaults(expanded_row, tables_dict)
    # todo: check ColX.not_null here. figure out what to do about null pkey field
    for i,elt in enumerate(row):
      # todo(awinter): think about dependency model if one field relies on another. (what do I mean? 'insert into t1 (a,b) values (10,a+5)'? is that valid?)
      row[i]=sqex.evalex(elt,row,nix,tables_dict)
    if self.pkey_get(row): raise pg.DupeInsert(row)
    self.rows.append(row)
    if returning: return sqex.evalex(returning,(row,),nix,tables_dict)
  def match(self,where,tables,nix):
    return [r for r in self.rows if not where or threevl.ThreeVL.test(sqex.evalex(where,(r,),nix,tables))]
  def lookup(self,name):
    if isinstance(name,sqparse2.NameX): name = name.name # this is horrible; be consistent
    try: return FieldLookup(*next((i,f) for i,f in enumerate(self.fields) if f.name==name))
    except StopIteration: raise BadFieldName(name)
  def update(self,setx,where,returning,tables_dict):
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    if not all(isinstance(x,sqparse2.AssignX) for x in setx): raise TypeError('not_xassign',map(type,setx))
    match_rows=self.match(where,tables_dict,nix) if where else self.rows
    for row in match_rows:
      for x in setx: row[self.lookup(x.col).index]=sqex.evalex(x.expr,(row,),nix,tables_dict)
    if returning: return sqex.evalex(returning,(row,),nix,tables_dict)
  def delete(self,where,tables_dict):
    # todo: what's the deal with nested selects in delete. does it get evaluated once to a scalar before running the delete?
    # todo: this will crash with empty where clause
    nix = sqex.NameIndexer.ctor_name(self.name)
    nix.resolve_aonly(tables_dict,Table)
    self.rows=[r for r in self.rows if not sqex.evalex(where,(r,),nix,tables_dict)]

def apply_sql(ex,values,tables_dict,cursor):
  "call the stmt in tree with values subbed on the tables in t_d\
  tree is a parsed statement returned by parse_expression. values is the tuple of %s replacements. tables_dict is a dictionary of Table instances."
  sqex.depth_first_sub(ex,values)
  with tables_dict.lock_db(cursor, isinstance(ex,sqparse2.StartX)):
    sqex.replace_subqueries(ex,tables_dict,Table)
    if isinstance(ex,sqparse2.SelectX): return sqex.run_select(ex,tables_dict,Table)
    elif isinstance(ex,sqparse2.InsertX): return tables_dict[ex.table].insert(ex.cols,ex.values,ex.ret,tables_dict)
    elif isinstance(ex,sqparse2.UpdateX):
      if len(ex.tables)!=1: raise NotImplementedError('multi-table update')
      return tables_dict[ex.tables[0]].update(ex.assigns,ex.where,ex.ret,tables_dict)
    elif isinstance(ex,sqparse2.CreateX):
      if ex.name in tables_dict: raise ValueError('table_exists',ex.name)
      if any(c.pkey for c in ex.cols): raise NotImplementedError('inline pkey')
      tables_dict[ex.name]=Table(ex.name,ex.cols,ex.pkey.fields if ex.pkey else [])
    elif isinstance(ex,sqparse2.IndexX): pass
    elif isinstance(ex,sqparse2.DeleteX): return tables_dict[ex.table].delete(ex.where,tables_dict)
    elif isinstance(ex,sqparse2.StartX): tables_dict.trans_start(cursor)
    elif isinstance(ex,sqparse2.CommitX): tables_dict.trans_commit()
    elif isinstance(ex,sqparse2.RollbackX): tables_dict.trans_rollback()
    else: raise TypeError(type(ex)) # pragma: no cover

class CursorMock(pg.Cursor):
  def __init__(self,poolmock): self.poolmock = poolmock; self.lastret = None
  def execute(self,qstring,vals=()):
    self.lastret = apply_sql(sqparse2.parse(qstring), vals, self.poolmock.tables, self)
    return len(self.lastret) if isinstance(self.lastret,list) else None
  def __iter__(self): return iter(self.lastret)
  def fetchone(self): return self.lastret[0]
  def __enter__(self):
    # todo: should transactions be happening with-ing the cursor or connection? I'm thinking the latter
    self.execute('start transaction')
    return self
  def __exit__(self, etype, evalue, tb): self.execute('commit' if etype is None else 'rollback')

class ConnectionMock:
  "for supporting the contextmanager call"
  def __init__(self,poolmock): self.poolmock=poolmock
  @contextlib.contextmanager
  def cursor(self): yield CursorMock(self.poolmock)

class PgPoolMock(pg.Pool): # only inherits so isinstance tests pass
  def __init__(self): self.tables=TablesDict()
  def select(self,qstring,vals=()): return apply_sql(sqparse2.parse(qstring),vals,self.tables,None)
  def commit(self,qstring,vals=()): return apply_sql(sqparse2.parse(qstring),vals,self.tables,None)
  def commitreturn(self,qstring,vals=()): return apply_sql(sqparse2.parse(qstring),vals,self.tables,None)[0]
  def close(self): pass
  @contextlib.contextmanager
  def __call__(self): yield ConnectionMock(self)
  @contextlib.contextmanager
  def withcur(self):
    with CursorMock(self) as cursor: yield cursor
