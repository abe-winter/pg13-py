"dbapi2 interface to pgmock"

import functools
from . import pgmock

# globals
apilevel = '2.0'
threadsafety = 1 # 1 means module-level. I think pgmock with transaction locking as-is is fully threadsafe, so write tests and bump this to 3.
paramstyle = 'format'

# global dictionary of databases (necessary so different connections can access the same DB)
DATABASES = {}
NEXT_DB_ID = 0

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

class Cursor:
  def __init__(self, connection):
    self.connection = connection
    self.rowcount = -1
    self.arraysize = 1
    self.rows = None
    self.rownumber = None
  @property
  def rowcount(self): return -1 if self.rows is None else len(self.rows)
  @property
  def description(self):
    "this is only a property so it can raise; make it an attr once it works"
    if self.rows is None: return None
    raise NotImplementedError
  def callproc(self, procname, parameters=None): raise NotImplementedError("pgmock doesn't support stored procs yet")
  def __del__(self): self.close()
  def close(self): pass # for now pgmock doesn't have cursor resources to close
  def execute(self, operation, parameters=None):
    raise NotImplementedError
    # self.rows = self.connection. ...
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
    if self.rows is None: raise Error('empty')
    ret, self.rows = self.rows, None
    return ret
  def nextset(self): raise NotImplementedError('are we supporting multi result sets?')
  def setinputsizes(self, sizes): raise NotImplementedError
  def setoutputsize(self, size, column=None): raise NotImplementedError
  def __enter__(self): return self
  def __exit__(self,etype,error,tb): self.close()
  def scroll(self, value, mode='relative'): raise NotImplementedError

def open_only(f):
  "decorator"
  @functools.wraps(f)
  def f2(self, *args, **kwargs):
    if self.closed: raise NotSupportedError('connection is closed')
    return f(self, *args, **kwargs)
  return f2

class Connection:
  # todo: does this need autocommit and begin()?
  def __init__(self, db_id):
    "pass None as db_id to create a new pgmock database"
    self.closed = False
    raise NotImplementedError
  @open_only
  def close(self):
    self.rollback()
    self.closed = True
  def __del__(self): self.close() # todo: can __del__ get called twice?
  @open_only
  def commit(self): raise NotImplementedError
  @open_only
  def rollback(self): raise NotImplementedError
  @open_only
  def cursor(self): return Cursor(self)
  @open_only
  def __enter__(self): return self
  @open_only
  def __exit__(self,etype,error,tb):
    (self.commit if etype is None else self.rollback)()

connect = Connection
