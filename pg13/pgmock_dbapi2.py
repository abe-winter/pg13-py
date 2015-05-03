"dbapi2 interface to pgmock"

import functools, contextlib, collections
from . import pgmock, sqparse2, pg

# globals
apilevel = '2.0'
threadsafety = 1 # 1 means module-level. I think pgmock with transaction locking as-is is fully threadsafe, so write tests and bump this to 3.
paramstyle = 'format'

# global dictionary of databases (necessary so different connections can access the same DB)
DATABASES = {}
NEXT_DB_ID = 0

def add_db():
  global NEXT_DB_ID
  db_id, NEXT_DB_ID = NEXT_DB_ID, NEXT_DB_ID + 1
  DATABASES[db_id] = pgmock.TablesDict()
  print 'created db %i' % db_id
  return db_id

# todo: catch pgmock errors and raise these
class Error(StandardError): pass
class InterfaceError(Error): pass
class DatabaseError(Error): pass
class DataError(DatabaseError): pass
class OperationalError(DatabaseError): pass
class IntegrityError(DatabaseError): pass
class InternalError(DatabaseError): pass
class ProgrammingError(DatabaseError): pass
class NotSupportedError(DatabaseError): pass

# types
class PGMockType(object): "base"
class Date(PGMockType):
  def __init__(year,month,day): pass
class Time(PGMockType):
  def __init__(hour,minute,second): pass
class Timestamp(PGMockType):
  def __init__(year,month,day,hour,minute,second): pass
class DateFromTicks(PGMockType):
  def __init__(ticks): pass
class TimeFromTicks(PGMockType):
  def __init__(ticks): pass
class TimestampFromTicks(PGMockType):
  def __init__(ticks): pass
class Binary(PGMockType):
  def __init__(string): pass
class STRING(PGMockType): pass
class BINARY(PGMockType): pass
class NUMBER(PGMockType): pass
class DATETIME(PGMockType): pass
class ROWID(PGMockType): pass

def expression_type(con, topx, ex):
  "take a BaseX descendant from sqparse2, return a type class from above"
  if isinstance(ex,sqparse2.Literal):
    if isinstance(ex.val,basestring): return STRING
    else: raise NotImplementedError('literal', type(ex.val))
  elif isinstance(ex,sqparse2.AttrX):
    if ex.parent.name in con.db: # warning: what if it's not a table? what if it's aliased?
      return expression_type(con, topx, con.db[ex.parent.name].get_column(ex.attr.name).coltp)
    else:
      raise NotImplementedError(ex.parent)
  elif isinstance(ex,sqparse2.TypeX):
    return dict(
      integer=NUMBER,
      text=STRING,
    )[ex.type.lower()]
  else:
    raise NotImplementedError('unk type', type(ex))

Description = collections.namedtuple('Description','name type_code display_size internal_size precision scale null_ok')
def description_from_colx(con, ex, colx):
  if isinstance(colx,sqparse2.AliasX):
    return Description(colx.alias,expression_type(con, ex, colx.name),*(None,)*5)
  elif isinstance(colx,sqparse2.NameX):
    raise NotImplementedError
    # return Description(,*(None,)*5)
  else: raise NotImplementedError(ex) # probably math expressions and anonymous fields

class Cursor(pg.Cursor):
  def __init__(self, connection):
    self.connection = connection
    self.arraysize = 1
    self.rows = None
    self.rownumber = None
    self.lastx = None
    # todo: self.lastrowid. SQLAlchemy uses it.
  @property
  def rowcount(self): return -1 if self.rows is None else len(self.rows)
  @property
  def description(self):
    "this is only a property so it can raise; make it an attr once it works"
    if self.lastx is None: return
    if type(self.lastx) not in (sqparse2.SelectX,sqparse2.UpdateX,sqparse2.InsertX): return
    if type(self.lastx) in (sqparse2.UpdateX,sqparse2.InsertX) and self.lastx.ret is None: return
    # at this point we know this is an operation that returns rows
    if type(self.lastx) in (sqparse2.UpdateX,sqparse2.InsertX):
      raise NotImplementedError('todo: Cursor.description for non-select')
    else: # select case
      return [description_from_colx(self.connection,self.lastx,colx) for colx in self.lastx.cols.children]
  def callproc(self, procname, parameters=None): raise NotImplementedError("pgmock doesn't support stored procs yet")
  def __del__(self): self.close()
  def close(self): pass # for now pgmock doesn't have cursor resources to close
  def execute(self, operation, parameters=None):
    ex = self.lastx = sqparse2.parse(operation)
    if not self.connection.transaction_open and not self.connection.autocommit:
      self.connection.begin()
    self.rows = self.connection.db.apply_sql(ex, parameters or (), self.connection)
    self.rownumber = 0 # always?
  def executemany(self, operation, seq_of_parameters):
    for param in seq_of_parameters: self.execute(operation, param)
  def fetchone(self):
    try: ret = self.rows[self.rownumber]
    except: raise # todo: wrap?
    else: self.rownumber += 1
    return ret
  def fetchmany(self, size=None): raise NotImplementedError # hoping nobody cares about this one
  def fetchall(self):
    if self.rows is None: raise Error('no query to fetch')
    ret, self.rows = self.rows, None
    return ret
  def nextset(self): raise NotImplementedError('are we supporting multi result sets?')
  def setinputsizes(self, sizes): raise NotImplementedError
  def setoutputsize(self, size, column=None): raise NotImplementedError
  def __enter__(self): return self
  def __exit__(self,etype,error,tb): self.close()
  def scroll(self, value, mode='relative'): raise NotImplementedError
  def __iter__(self):
    rownum, self.rownumber = self.rownumber, None
    return iter(self.rows[rownum:])

def open_only(f):
  "decorator"
  @functools.wraps(f)
  def f2(self, *args, **kwargs):
    if self.closed: raise NotSupportedError('connection is closed')
    return f(self, *args, **kwargs)
  return f2

class Connection:
  # todo: does this need autocommit and begin()?
  def __init__(self, db_id=None):
    "pass None as db_id to create a new pgmock database"
    self.closed = False
    self.db_id = add_db() if db_id is None else db_id
    self.db = DATABASES[self.db_id]
    print 'connected db %i' % self.db_id
    self._autocommit = False
    self.transaction_open = False
  @property
  def autocommit(self): return self._autocommit
  @autocommit.setter
  def autocommit(self, value):
      if value and self.transaction_open: raise OperationalError("can't set autocommit with open transaction")
      self._autocommit = value
  @open_only
  def close(self):
    if self.transaction_open: self.rollback()
    self.closed = True
    self.db_id = None
    self.db = None
  def __del__(self):
    if not self.closed: self.close()
  @open_only
  def begin(self):
    if self.transaction_open: raise OperationalError("can't begin() with transaction_open")
    self.db.trans_start(self)
    self.transaction_open = True
  @open_only
  def commit(self):
    if not self.transaction_open: raise OperationalError("can't commit without transaction_open")
    self.db.trans_commit()
    self.transaction_open = False
    print 'commit'
  @open_only
  def rollback(self):
    if not self.transaction_open: raise OperationalError("can't rollback without transaction_open")
    self.db.trans_rollback()
    self.transaction_open = False
    print 'rollback'
  @open_only
  def cursor(self): return Cursor(self)
  @open_only
  def __enter__(self):
    self.begin()
    return self
  @open_only
  def __exit__(self,etype,error,tb):
    (self.commit if etype is None else self.rollback)()

connect = Connection

def call_cur(f):
  "decorator for opening a connection and passing a cursor to the function"
  @functools.wraps(f)
  def f2(self, *args, **kwargs):
    with self.withcur() as cur:
      return f(self, cur, *args, **kwargs)
  return f2

class PgPoolMock(pg.Pool): # only inherits so isinstance tests pass
  def __init__(self):
    self.db_id = add_db()
  @property
  def tables(self): return DATABASES[self.db_id]
  @call_cur
  def select(self, cursor, qstring, vals=()):
    "careful: don't pass cursor (it's from decorator)"
    cursor.execute(qstring, vals) # hmm; do I not want to commit at the end of this?
    return cursor.fetchall()
  @call_cur
  def commit(self, cursor, qstring, vals=()):
    "careful: don't pass cursor (it's from decorator)"
    cursor.execute(qstring, vals)
  @call_cur
  def commitreturn(self, cursor, qstring, vals=()):
    "careful: don't pass cursor (it's from decorator)"
    cursor.execute(qstring, vals)
    return cursor.fetchall()[0]
  def close(self): pass # todo: is this closeall?
  @contextlib.contextmanager
  def __call__(self):
    with Connection(self.db_id) as con:
      yield con
  @contextlib.contextmanager
  def withcur(self):
    "don't pass cursor"
    with self() as con, con.cursor() as cur:
      yield cur
