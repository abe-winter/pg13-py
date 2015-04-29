"To use pg13 for ORM, inherit your model classes from Row."
# todo: make everything take a pool_or_cursor instead of just a pool (at least make them all check for it)
# todo: don't allow JSONFIELDS to overlap with primary key? think about it. >> is this obsolete with SpecialField?
# todo: add profiling hooks

import sys,contextlib,ujson,functools,collections
from . import errors

class Select1Error(StandardError): "base for select1 error conditions"
class Missing(Select1Error): pass
class NotUnique(Select1Error): pass
class SchemaError(StandardError): "base for schema-related errors"
class FieldError(SchemaError): "no such field in model"
class NullJsonError(StandardError): pass
class DupeInsert(StandardError): pass

def eqexpr(key,value):
  "for automatic x is null vs x=value stmts"
  return key+(' is %s' if value is None else '=%s')

class CheckedCollection(object):
  ""
  def __init__(self, collection_type, item_type):
    raise NotImplementedError

class Cursor(object):
  "based class for cursor wrappers. necessary for error-wrapping."
  def execute(self,qstring,vals=()): raise NotImplementedError
  def __iter__(self): raise NotImplementedError
  def fetchone(self): raise NotImplementedError

class Pool(object):
  # todo: DBAPI compatibility https://www.python.org/dev/peps/pep-0249/
  """
  This is the base class for pool wrappers. Most of the Row methods expect one of these as the first argument.

  Here's an example of how to construct one to connect to your database::

    from pg13 import pool_psyco
    def mkpool():
      dets=dict(
        host='127.0.0.1',
        dbname='yourdb',
        user='username',
        password='topsecret',
      )
      return pool_psyco.PgPoolPsyco(' '.join("%s='%s'"%(k,v) for k,v in dets.items()))

  Careful: this isn't DBAPI compatible. In particular commit() doesn't do what you expect.
  """
  def __init__(self,dbargs): raise NotImplementedError
  def select(self,qstring,vals=()): raise NotImplementedError
  def commit(self,qstring,vals=()): raise NotImplementedError
  def commitreturn(self,qstring,vals=()): raise NotImplementedError
  def close(self): raise NotImplementedError
  @contextlib.contextmanager
  def __call__(self): raise NotImplementedError

def is_serdes(x):
  "todo: once there's a SerDes based class, replace all calls with isinstance()"
  return hasattr(x,'ser') and hasattr(x,'des')

def transform_specialfield((f,v)):
  "helper for serialize_row"
  return f.ser(v) if is_serdes(f) else v

def dirty(field,ttl=None):
  "decorator to cache the result of a function until a field changes"
  if ttl is not None: raise NotImplementedError('pg.dirty ttl feature')
  def decorator(f):
    @functools.wraps(f)
    def wrapper(self,*args,**kwargs):
      # warning: not reentrant
      d=self.dirty_cache[field] if field in self.dirty_cache else self.dirty_cache.setdefault(field,{})
      return d[f.__name__] if f.__name__ in d else d.setdefault(f.__name__,f(self,*args,**kwargs))
    return wrapper
  return decorator

def commit_or_execute(pool_or_cursor,qstring,vals=()):
  if isinstance(pool_or_cursor,Pool): pool_or_cursor.commit(qstring,vals)
  elif isinstance(pool_or_cursor,Cursor): pool_or_cursor.execute(qstring,vals)
  else: raise TypeError('bad_pool_or_cursor_type',type(pool_or_cursor))
def select_or_execute(pool_or_cursor,qstring,vals=()):
  if isinstance(pool_or_cursor,Pool): return pool_or_cursor.select(qstring,vals)
  elif isinstance(pool_or_cursor,Cursor):
    pool_or_cursor.execute(qstring,vals)
    return pool_or_cursor
  else: raise TypeError('bad_pool_or_cursor_type',type(pool_or_cursor))
def commitreturn_or_fetchone(pool_or_cursor,qstring,vals=()):
  if isinstance(pool_or_cursor,Pool): return pool_or_cursor.commitreturn(qstring,vals)
  elif isinstance(pool_or_cursor,Cursor):
    pool_or_cursor.execute(qstring,vals)
    return pool_or_cursor.fetchone()

class Row(object):
  """Base class for models.
  
  :cvar FIELDS: list of (name,sql_type_string) tuples. pg13 doesn't know anything about sql types except for SpecialField (which you shouldn't use yet). So you can use anything psycopg2 knows how to translate.
  :cvar PKEY: string with comma-separated names. This will probably become a tuple in the future.
  :cvar INDEXES: list of strings. Each string can be one of:

    * string of comma-separated field names, which will be wrapped into a 'create index' statement.
    * string starting with 'create index' which will be run as-is.

  :cvar TABLE: string, table name.
  :cvar REFKEYS: don't use this yet. It's for automatic child detection in client-server interactions.

  Example::

    class Model(pg.Row):
      FIELDS = [('a','int'),('b','int'),('c','text')]
      PKEY = 'a,b'
      TABLE = 'model'
      @pg.dirty('c') # the dirty decorator caches computations until the field is changed
      def textlen(self):
        return len(self['c'])
      def aplusb(self):
        return self['a'] + self['b'] # note how __getitem__ (square brackets) is used to access fields
  """
  # todo: metaclass stuff to check field names on class creation? forbidden column names: returning
  FIELDS = []
  PKEY = ''
  INDEXES = []
  TABLE = ''
  REFKEYS={} # this is used by syncschema. see syncschema.py for usage.
  SENDRAW = [] # used by syncschema to send non-syncable fields 'raw'
  @classmethod
  def create_indexes(clas,pool_or_cursor):
    "this gets called by create_table, but if you've created an index you can use it to add it (assuming none exist)"
    for index in clas.INDEXES:
      # note: these are specified as either 'field,field,field' or a runnable query. you can put any query you want in there
      query = index if 'create index' in index.lower() else 'create index on %s (%s)'%(clas.TABLE,index)
      commit_or_execute(pool_or_cursor,query)
  @classmethod
  def create_table(clas,pool_or_cursor):
    """Use FIELDS, PKEY, INDEXES and TABLE members to create a sql table for the model.

    .. warning:: this uses 'if not exists' so if you've updated your model, you need to delete the old table first.
    (even better, increment the TABLE member from 'mymodel' to 'mymodel2')
    """
    print clas.__name__,'create_table'
    def mkfield((name,tp)): return name,(tp if isinstance(tp,basestring) else 'jsonb')
    fields = ','.join(map(' '.join,map(mkfield,clas.FIELDS)))
    base = 'create table if not exists %s (%s'%(clas.TABLE,fields)
    if clas.PKEY: base += ',primary key (%s)'%clas.PKEY
    base += ')'
    commit_or_execute(pool_or_cursor,base)
    clas.create_indexes(pool_or_cursor)
  @classmethod
  def names(class_): "helper; returns list of the FIELD names"; return zip(*class_.FIELDS)[0]
  def __eq__(self,other): return isinstance(other,type(self)) and all(self[k]==other[k] for k in self.names())
  def __neq__(self,other): return not self==other
  def __init__(self,*cols):
    "note: __init__ takes strings for SpecialField fields instead of the deserialized object because it expects to construct from DB rows"
    if len(cols)!=len(self.FIELDS): raise ValueError(len(cols),len(self.FIELDS))
    self.values=list(cols)
    self.dirty_cache={}
  def __getitem__(self,name):
    "note: supporting nulls here is complicated and I'm not sure it's the right thing. I guess *not* supporting them can break some inserts.\
    Converting nulls to empties in Row.insert() will solve some cases."
    if name is Ellipsis: return self.values
    try: index=self.index(name)
    except ValueError: raise FieldError("%s.%s"%(self.__class__.__name__,name))
    val = self.values[index]
    field = self.FIELDS[index][1]
    # todo: typecheck val on readback
    return field.des(val) if is_serdes(field) else val
  @classmethod
  def index(class_,name): "helper; returns index of field name in row"; return class_.names().index(name)
  @classmethod
  def pkey_get(clas,pool_or_cursor,*vals):
    "lookup by primary keys in order"
    pkey = clas.PKEY.split(',')
    if len(vals)!=len(pkey): raise ValueError("%i args != %i-len primary key for %s"%(len(vals),len(pkey),clas.TABLE))
    rows = list(clas.select(pool_or_cursor,**dict(zip(pkey,vals))))
    if not rows: raise Missing
    return clas(*rows[0])
  @classmethod
  def pkey_get_withref(clas,pool_or_cursor,*vals):
    "get the thing and the stuff from REFKEYS in a single roundtrip"
    # create an array_agg per thing in REFKEYS.
    # this requires a DB stored proc (or a really complicated select) to unpack the versioned fields.
    raise NotImplementedError('todo') # pragma: no cover
  @classmethod
  def select(clas,pool_or_cursor,**kwargs):
    """
    :param Pool pool_or_cursor: can also be a psycopg2 cursor.
    :param kwargs: see examples above.
    :return: generator yielding raw rows (i.e. generator of lists)

    .. seealso:: select_models

    Example::

      Model.select(pool, a=1, b=2)

    That would get translated into 'select * from tablename where a=1 and b=2'

    Be careful, the 'columns' keyword is treated specially::

      Model.select(pool, a=1, b=2, columns='a,c')

    That would run 'select a,c from tablename where a=1 and b=2'

    .. note:: This returns a generator, not a list. All your expectations will be violated.
    """
    columns = kwargs.pop('columns','*')
    # todo(awinter): write a test for whether eqexpr matters; figure out pg behavior and apply to pgmock
    query="select %s from %s"%(columns,clas.TABLE)
    if kwargs: query+=' where %s'%' and '.join('%s=%%s'%k for k in kwargs)
    return select_or_execute(pool_or_cursor,query,tuple(kwargs.values()))
  @classmethod
  def select_models(clas,pool_or_cursor,**kwargs):
    """
    Same input as Row.select (and calls it internally), different return.

    :return: generator yielding instances of the class.
    """
    if 'columns' in kwargs: raise ValueError("don't pass 'columns' to select_models")
    return (clas(*row) for row in clas.select(pool_or_cursor,**kwargs))
  @classmethod
  def selectwhere(clas,pool_or_cursor,userid,qtail,vals=(),needs_and=True):
    """
    Shortcut for passing in the part of the query after 'select * from table where '. This is 'multitenant' in the sense that it demands a userid column.

    Example::

      Model.selectwhere(pool,15,'a-b=%s+1',(50,))

    That gets translated into 'select * from tablename where userid=15 and a-b=50-1'

    :return: generator yielding instances of the class.

    .. seealso:: select_xiny
    """
    qstring=('select * from %s where userid=%i '%(clas.TABLE,userid))+('and ' if needs_and else '')+qtail
    return (clas(*row) for row in select_or_execute(pool_or_cursor, qstring, tuple(vals)))
  @classmethod
  def select_xiny(clas,pool_or_cursor,userid,field,values):
    """
    'Select X in Y'. selectwhere shortcut for 'docid in (1,2,3)' queries. Assumes multitenancy with 'userid' column.

    :param field str:
    :param values list:

    Examples::

      Model.select_xiny(pool,15,'docid',[1,2,3])
      Model.select_xiny(pool,15,'(docid,subdocid)',[(1,2),(1,3)]) # I'm not sure if the parens around the field are necessary here
    """
    return clas.selectwhere(pool_or_cursor,userid,'%s in %%s'%field,(tuple(values),)) if values else []
  @classmethod
  def serialize_row(clas,fieldnames,vals):
    "I think this is only relevant if you're using SpecialField"
    fieldtypes=[clas.FIELDS[clas.index(name)][1] for name in fieldnames]
    return tuple(map(transform_specialfield,zip(fieldtypes,vals)))
  @classmethod
  def insert_all(clas,pool_or_cursor,*vals):
    """
    Use this to create a new row by passing all fields.
    :param vals: must be the same length as clas.FIELDS
    :return: an instance of the class *as returned by the DB*; in other words, defaults are not being handled by wonky client logic.
    """
    # note: it would be nice to write this on top of clas.insert, but this returns an object and that has a returning feature. tricky semantics.
    if len(clas.FIELDS)!=len(vals): raise ValueError('fv_len_mismatch',len(clas.FIELDS),len(vals),clas.TABLE)
    vals=clas.serialize_row([f[0] for f in clas.FIELDS],vals)
    query = "insert into %s values (%s)"%(clas.TABLE,','.join(['%s']*len(vals)))
    try: commit_or_execute(pool_or_cursor, query, vals)
    except errors.PgPoolError as e:
      # todo: need cross-db, cross-version, cross-driver testing to get this right
      raise DupeInsert(clas.TABLE,e) # note: pgmock raises DupeInsert directly, so catching this works in caller. (but args are different)
    return clas(*vals)
  @classmethod
  def insert(clas,pool_or_cursor,fields,vals,returning=None):
    """
    Use this to create a new row without passing all fields (i.e. accepting defaults).
    Examples::

      Model.insert(pool,('a','b'),(1,2))
      Model.insert(pool,('a','b'),(1,2),'*') # returning [row_list]
      Model.insert(pool,('a','b'),(1,2),'c') # returning [[c]]

    :param fields iterable:
    :param vals iterable: must match len of fields
    :return: it *should* return an instance of the class but instead it returns whatever you ask it for with the returning field.
      I guess you can pass '*' to get a raw row back (i.e. a list) >> or might be a list of lists.
    """
    if len(fields)!=len(vals): raise ValueError('fv_len_mismatch',len(fields),len(vals),clas.TABLE)
    vals=clas.serialize_row(fields,vals)
    query = "insert into %s (%s) values (%s)"%(clas.TABLE,','.join(fields),','.join(['%s']*len(vals)))
    if returning: return commitreturn_or_fetchone(pool_or_cursor,query+' returning '+returning,vals)
    else: commit_or_execute(pool_or_cursor,query,vals)
  @classmethod
  def kwinsert(clas,pool_or_cursor,**kwargs):
    "kwargs version of insert"
    returning = kwargs.pop('returning',None)
    fields,vals = zip(*kwargs.items())
    # note: don't do SpecialField resolution here; clas.insert takes care of it
    return clas.insert(pool_or_cursor,fields,vals,returning=returning)
  @classmethod
  def checkdb(clas,pool_or_cursor): raise NotImplementedError("check that DB table matches our fields") # pragma: no cover
  @classmethod
  def insert_mtac(clas,pool_or_cursor,restrict,incfield,fields=(),vals=()):
    """
    multitenant auto-increment insert. restrict is a dict of keys and raw values to use for looking up the new incfield id.

    This needs better doc but there's an exampe on the github page.
    """
    if not isinstance(clas.FIELDS[clas.index(incfield)][1],basestring):
      raise TypeError('mtac_specialfield_unsupported','incfield',incfield)
    if any(not isinstance(clas.FIELDS[clas.index(f)][1],basestring) for f in restrict):
      raise TypeError('mtac_specialfield_unsupported','restrict')
    if len(fields)!=len(vals): raise ValueError("insert_mtac len(fields)!=len(vals)")
    vals=clas.serialize_row(fields,vals)
    where = ' and '.join('%s=%s'%tup for tup in restrict.items())
    mtac = '(select coalesce(max(%s),-1)+1 from %s where %s)'%(incfield,clas.TABLE,where)
    qcols = ','.join([incfield]+restrict.keys()+list(fields))
    # todo(awinter): are both vals ever empty? if yes this breaks.
    qvals = tuple(restrict.values())+tuple(vals)
    valstring = ','.join(['%s']*len(qvals))
    query = 'insert into %s (%s) values (%s,%s) returning *'%(clas.TABLE,qcols,mtac,valstring)
    return clas(*commitreturn_or_fetchone(pool_or_cursor,query,qvals))
  @classmethod
  def pkey_update(clas,pool_or_cursor,pkey_vals,escape_keys,raw_keys=None):
    """raw_keys vs escape_keys -- raw_keys doesn't get escaped, so you can do something like column=column+1

    Example::

      Model.pkey_update(pool,(1,2),{'counter':10}) # update tablename set counter=10 where a=1 and b=2
      Model.pkey_update(pool,(1,2),{},{'counter':'counter+1'}) # the last arg is raw_keys. this *doesn't get escaped* so the DB will actually increment the field.
    """
    if not clas.PKEY: raise ValueError("can't update %s, no primary key"%clas.TABLE)
    if any(not isinstance(clas.FIELDS[clas.index(f)][1],basestring) for f in (raw_keys or ())):
      raise TypeError('rawkeys_specialfield_unsupported')
    escape_keys=dict(zip(escape_keys,clas.serialize_row(escape_keys,escape_keys.values())))
    pkey = clas.PKEY.split(',')
    raw_keys=raw_keys or {}
    if any(k in pkey for k in list(escape_keys)+list(raw_keys)): raise ValueError("pkey field updates not allowed") # todo: why?
    if len(pkey_vals)!=len(pkey): raise ValueError("len(pkey_vals) %i != len(pkey) %i"%(len(pkey_vals),len(pkey)))
    # todo(awinter): if I'm going to allow SpecialField in primary key vals, serialize here
    whereclause=' and '.join('%s=%%s'%k for k in pkey)
    setclause=','.join(['%s=%%s'%k for k in escape_keys]+['%s=%s'%tup for tup in raw_keys.items()])
    # note: raw_keys could contain %s as well as a lot of other poison
    query='update %s set %s where %s'%(clas.TABLE,setclause,whereclause)
    vals=tuple(escape_keys.values())+pkey_vals
    if raw_keys: return commitreturn_or_fetchone(pool_or_cursor,query+' returning '+','.join(raw_keys),vals)
    else: commit_or_execute(pool_or_cursor,query,vals)
  def pkey_vals(self):
    """
    :return: tuple of the instance's primary key (the values, not the field names)
    """
    if not hasattr(self,'PKEY') or not self.PKEY: raise KeyError("no primary key on %s"%self.TABLE)
    return tuple(map(self.__getitem__,self.PKEY.split(',')))
  def update(self,pool_or_cursor,escape_keys,raw_keys=None):
    """
    Same as Row.pkey_update except it works on an instance.

    The instance's fields are updated. Row has no __setitem__ (intentionally); this is the only sanctioned way to edit values and it requires a database roundtrip.

    Example::

      model_instance.update(pool,{},{'counter':'counter+1'})
    """
    pkey_vals=self.pkey_vals()
    # note: do not serialize SpecialField. pkey_update takes care of it.
    rawvals=self.pkey_update(pool_or_cursor,pkey_vals,escape_keys,raw_keys)
    # note: Row has no __setitem__ because this is the only time we want to modify our copy of data (after an update to reflect DB)
    if raw_keys:
      for k,v in zip(raw_keys,rawvals): self.values[self.index(k)]=v # this is necessary because raw_keys can contain expressions
      map(self.dirty_cache.pop,[k for k in raw_keys if k in self.dirty_cache])
    escape_keys=dict(zip(escape_keys,self.serialize_row(escape_keys,escape_keys.values()))) # ugly; doing it in pkey_update and this
    for k,v in escape_keys.items(): self.values[self.index(k)]=v
    map(self.dirty_cache.pop,[k for k in escape_keys if k in self.dirty_cache])
  @classmethod
  def updatewhere(clas,pool_or_cursor,where_keys,**update_keys):
    "this doesn't allow raw_keys for now"
    # if clas.JSONFIELDS: raise NotImplementedError # todo(awinter): do I need to make the same change for SpecialField?
    if not where_keys or not update_keys: raise ValueError
    setclause=','.join(k+'=%s' for k in update_keys)
    whereclause=' and '.join(eqexpr(k,v) for k,v in where_keys.items())
    q='update %s set %s where %s'%(clas.TABLE,setclause,whereclause)
    vals = tuple(update_keys.values()+where_keys.values())
    commit_or_execute(pool_or_cursor,q,vals)
  def delete(self,pool_or_cursor):
    ".. warning:: pgmock doesn't support delete yet, so this isn't tested"
    vals=self.pkey_vals()
    whereclause=' and '.join('%s=%%s'%k for k in self.PKEY.split(','))
    q='delete from %s where %s'%(self.TABLE,whereclause)
    commit_or_execute(pool_or_cursor,q,vals)
  def clientmodel(self): # pragma: no cover
    "Use the class's CLIENTFIELDS attribute to create a dictionary the client can read. Don't use this."
    raise NotImplementedError # todo delete: this is only used in dead pinboard code and a test
    return {k:self[k] for k in self.CLIENTFIELDS}
  def refkeys(self,fields):
    "returns {ModelClass:list_of_pkey_tuples}. see syncschema.RefKey. Don't use this yet."
    # todo doc: better explanation of what refkeys are and how fields plays in
    dd=collections.defaultdict(list)
    if any(f not in self.REFKEYS for f in fields): raise ValueError(fields,'not all in',self.REFKEYS.keys())
    for f in fields:
      rk=self.REFKEYS[f]
      for model in rk.refmodels: dd[model].extend(rk.pkeys(self,f))
    return dd
  def __repr__(self):
    pkey = ' %s'%','.join('%s:%s'%(k,self[k]) for k in self.PKEY.split(',')) if self.PKEY else ''
    return '<%s(pg.Row)%s>'%(self.__class__.__name__,pkey)
