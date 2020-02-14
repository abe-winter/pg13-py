"To use pg13 for ORM, inherit your model classes from Row."
# todo: make everything take a pool_or_cursor instead of just a pool (at least make them all check for it)
# todo: don't allow JSONFIELDS to overlap with primary key? think about it. >> is this obsolete with SpecialField?
# todo: add profiling hooks
# todo: need deserialize for SELECT / RETURNING values according to JSON_WRITE, JSON_READ

import contextlib, json, functools, collections, re
from . import errors

class Select1Error(Exception):
  "base for select1 error conditions"
class Missing(Select1Error):
  pass
class NotUnique(Select1Error):
  pass
class SchemaError(Exception):
  "base for schema-related errors"
class FieldError(SchemaError):
  "no such field in model"
class NullJsonError(Exception):
  pass
class DupeInsert(Exception):
  pass

def eqexpr(key, value):
  "for automatic x is null vs x = value stmts"
  return key+(' is %s' if value is None else ' = %s')

class Cursor:
  "base class for cursor wrappers. necessary for error-wrapping."
  # see pool_psyco.py for docs on JSON_WRITE/JSON_READ
  JSON_WRITE = None
  JSON_READ = None
  def execute(self, qstring, vals=()):
    raise NotImplementedError
  def __iter__(self):
    raise NotImplementedError
  def fetchone(self):
    raise NotImplementedError

class Pool:
  "base class for pool wrappers. Most of the Row methods expect one of these as the first argument"
  # see pool_psyco.py for docs on JSON_WRITE/JSON_READ
  JSON_WRITE = None
  JSON_READ = None
  def __init__(self, dbargs):
    raise NotImplementedError
  def select(self, qstring, vals=()):
    raise NotImplementedError
  def commit(self, qstring, vals=()):
    raise NotImplementedError
  def commitreturn(self, qstring, vals=()):
    raise NotImplementedError
  def close(self):
    raise NotImplementedError
  @contextlib.contextmanager
  def __call__(self):
    raise NotImplementedError

def is_serdes(instance):
  "todo: once there's a SerDes based class, replace all calls with isinstance()"
  return hasattr(instance, 'ser') and hasattr(instance, 'des')

def set_options(pool_or_cursor, row_instance):
  "for connection-level options that need to be set on Row instances"
  # todo: move around an Options object instead
  for option in ('JSON_READ', ):
    setattr(row_instance, option, getattr(pool_or_cursor, option, None))
  return row_instance

def transform_specialfield(jsonify, field, value):
  "helper for serialize_row"
  raw = field.ser(value) if is_serdes(field) else value
  return json.dumps(raw) if not isinstance(field, str) and jsonify else raw

def dirty(field, ttl=None):
  "decorator to cache the result of a function until a field changes"
  if ttl is not None:
    raise NotImplementedError('pg.dirty ttl feature')
  def decorator(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
      # warning: not reentrant
      dict_ = self.dirty_cache[field] if field in self.dirty_cache else self.dirty_cache.setdefault(field, {})
      return dict_[func.__name__] if func.__name__ in dict_ else dict_.setdefault(func.__name__, func(self, *args, **kwargs))
    return wrapper
  return decorator

def commit_or_execute(pool_or_cursor, qstring, vals=()):
  if isinstance(pool_or_cursor, Pool):
    pool_or_cursor.commit(qstring, vals)
  elif isinstance(pool_or_cursor, Cursor):
    pool_or_cursor.execute(qstring, vals)
  else:
    raise TypeError('bad_pool_or_cursor_type', type(pool_or_cursor))

def select_or_execute(pool_or_cursor, qstring, vals=()):
  if isinstance(pool_or_cursor, Pool):
    return pool_or_cursor.select(qstring, vals)
  elif isinstance(pool_or_cursor, Cursor):
    pool_or_cursor.execute(qstring, vals)
    return pool_or_cursor
  else:
    raise TypeError('bad_pool_or_cursor_type', type(pool_or_cursor))

def commitreturn_or_fetchone(pool_or_cursor, qstring, vals=()):
  if isinstance(pool_or_cursor, Pool):
    return pool_or_cursor.commitreturn(qstring, vals)
  elif isinstance(pool_or_cursor, Cursor):
    pool_or_cursor.execute(qstring, vals)
    return pool_or_cursor.fetchone()
  else:
    raise TypeError('bad_pool_or_cursor_type', type(pool_or_cursor))

class Row:
  "base class for models"
  # todo: metaclass stuff to check field names on class creation? forbidden column names: returning
  # todo: metaclass for converting fields to a namedtuple
  FIELDS = []
  PKEY = ''
  INDEXES = []
  TABLE = ''
  REFKEYS = {} # this is used by syncschema. see syncschema.py for usage.
  SENDRAW = [] # used by syncschema to send non-syncable fields 'raw'
  JSON_READ = None

  @classmethod
  def create_indexes(cls, pool_or_cursor):
    "this gets called by create_table, but if you've created an index you can use it to add it (assuming none exist)"
    for index in cls.INDEXES:
      # note: these are specified as either 'field, field, field' or a runnable query. you can put any query you want in there
      query = index if 'create index' in index.lower() else 'create index on %s (%s)'%(cls.TABLE, index)
      commit_or_execute(pool_or_cursor, query)

  @classmethod
  def create_table(cls, pool_or_cursor):
    "uses FIELDS, PKEY, INDEXES and TABLE members to create a sql table for the model"
    def mkfield(pair):
      name, type_ = pair
      return name, (type_ if isinstance(type_, str) else 'jsonb')

    fields = ','.join(map(' '.join, list(map(mkfield, cls.FIELDS))))
    base = 'create table if not exists %s (%s' % (cls.TABLE, fields)
    if cls.PKEY:
      base += ', primary key (%s)' % cls.PKEY
    base += ')'
    commit_or_execute(pool_or_cursor, base)
    cls.create_indexes(pool_or_cursor)

  @classmethod
  def names(cls):
    "helper; returns list of the FIELDS names"
    return [name for name, val in cls.FIELDS]

  def __eq__(self, other):
    return isinstance(other, type(self)) and all(self[k] == other[k] for k in self.names())

  def __neq__(self, other):
    return not self == other

  def __init__(self, *cols):
    "note: __init__ takes strings for SpecialField fields instead of the deserialized object because it expects to construct from DB rows"
    if len(cols) != len(self.FIELDS):
      raise ValueError(len(cols), len(self.FIELDS))
    self.values = list(cols)
    self.dirty_cache = {}

  def __getitem__(self, name):
    "note: supporting nulls here is complicated and I'm not sure it's the right thing. I guess *not* supporting them can break some inserts.\
    Converting nulls to empties in Row.insert() will solve some cases."
    if name is Ellipsis:
      return self.values
    try:
      index = self.index(name)
    except ValueError:
      raise FieldError("%s.%s"%(self.__class__.__name__, name))
    val = self.values[index]
    field = self.FIELDS[index][1]
    # todo: typecheck val on readback
    parsed_val = json.loads(val) if isinstance(field, str) and self.JSON_READ else val
    return field.des(parsed_val) if is_serdes(field) else parsed_val

  @classmethod
  def index(cls, name):
    "helper; returns index of field name in row"
    return cls.names().index(name)

  @classmethod
  def split_pkey(cls):
    "get pkey, split by whitespace-agnostic comma"
    return re.split(r',\s*', cls.PKEY)

  @classmethod
  def pkey_get(cls, pool_or_cursor, *vals):
    "lookup by primary keys in order"
    pkey = cls.split_pkey()
    if len(vals) != len(pkey):
      raise ValueError("%i args != %i-len primary key for %s"%(len(vals), len(pkey), cls.TABLE))
    rows = list(cls.select(pool_or_cursor, **dict(list(zip(pkey, vals)))))
    if not rows:
      raise Missing
    return set_options(pool_or_cursor, cls(*rows[0]))

  @classmethod
  def pkey_get_withref(cls, pool_or_cursor, *vals):
    "get the thing and the stuff from REFKEYS in a single roundtrip"
    # create an array_agg per thing in REFKEYS.
    # this requires a DB stored proc (or a really complicated select) to unpack the versioned fields.
    raise NotImplementedError('todo') # pragma: no cover

  @classmethod
  def select(cls, pool_or_cursor, **kwargs):
    "note: This returns a generator, not a list. All your expectations will be violated"
    columns = kwargs.pop('columns', '*')
    # todo: write a test for whether eqexpr matters; figure out pg behavior and apply to pgmock
    query = "select %s from %s"%(columns, cls.TABLE)
    if kwargs:
      query += ' where %s' % ' and '.join('%s = %%s' % k for k in kwargs)
    return select_or_execute(pool_or_cursor, query, tuple(kwargs.values()))

  @classmethod
  def select_models(cls, pool_or_cursor, **kwargs):
    "returns generator yielding instances of the class"
    if 'columns' in kwargs:
      raise ValueError("don't pass 'columns' to select_models")
    return (set_options(pool_or_cursor, cls(*row)) for row in cls.select(pool_or_cursor, **kwargs))

  @classmethod
  def selectwhere(cls, pool_or_cursor, userid, qtail, vals=(), needs_and=True):
    qstring = ('select * from %s where userid = %i '%(cls.TABLE, userid))+('and ' if needs_and else '')+qtail
    return (set_options(pool_or_cursor, cls(*row)) for row in select_or_execute(pool_or_cursor, qstring, tuple(vals)))

  @classmethod
  def select_xiny(cls, pool_or_cursor, userid, field, values):
    return cls.selectwhere(pool_or_cursor, userid, '%s in %%s'%field, (tuple(values), )) if values else []

  @classmethod
  def serialize_row(cls, pool_or_cursor, fieldnames, vals, for_read=False):
    fieldtypes = [cls.FIELDS[cls.index(name)][1] for name in fieldnames]
    jsonify = pool_or_cursor.JSON_READ if for_read else pool_or_cursor.JSON_WRITE
    return tuple(transform_specialfield(jsonify, ft, v) for ft, v in zip(fieldtypes, vals))

  @classmethod
  def insert_all(cls, pool_or_cursor, *vals):
    # note: it would be nice to write this on top of cls.insert, but this returns an object and that has a returning feature. tricky semantics.
    if len(cls.FIELDS) != len(vals):
      raise ValueError('fv_len_mismatch', len(cls.FIELDS), len(vals), cls.TABLE)
    serialized_vals = cls.serialize_row(pool_or_cursor, [f[0] for f in cls.FIELDS], vals)
    query = "insert into %s values (%s)"%(cls.TABLE, ','.join(['%s']*len(serialized_vals)))
    try:
      commit_or_execute(pool_or_cursor, query, serialized_vals)
    except errors.PgPoolError as err:
      # todo: need cross-db, cross-version, cross-driver testing to get this right
      raise DupeInsert(cls.TABLE, err) # note: pgmock raises DupeInsert directly, so catching this works in caller. (but args are different)
    return set_options(
      pool_or_cursor,
      cls(*cls.serialize_row(pool_or_cursor, cls.names(), vals, for_read=True))
    )

  @classmethod
  def insert(cls, pool_or_cursor, fields, vals, returning=None):
    if len(fields) != len(vals):
      raise ValueError('fv_len_mismatch', len(fields), len(vals), cls.TABLE)
    vals = cls.serialize_row(pool_or_cursor, fields, vals)
    query = "insert into %s (%s) values (%s)"%(cls.TABLE, ','.join(fields), ', '.join(['%s']*len(vals)))
    if returning:
      return commitreturn_or_fetchone(pool_or_cursor, query + ' returning ' + returning, vals)
    else:
      commit_or_execute(pool_or_cursor, query, vals)
      return None

  @classmethod
  def kwinsert(cls, pool_or_cursor, **kwargs):
    "kwargs version of insert"
    returning = kwargs.pop('returning', None)
    fields, vals = list(zip(*list(kwargs.items())))
    # note: don't do SpecialField resolution here; cls.insert takes care of it
    return cls.insert(pool_or_cursor, fields, vals, returning=returning)

  @classmethod
  def kwinsert_mk(cls, pool_or_cursor, **kwargs):
    "wrapper for kwinsert that returns a constructed class. use this over kwinsert in most cases"
    if 'returning' in kwargs:
      raise ValueError("don't call kwinsert_mk with 'returning'")
    return set_options(
      pool_or_cursor,
      cls(*cls.kwinsert(pool_or_cursor, returning='*', **kwargs))
    )

  @classmethod
  def checkdb(cls, pool_or_cursor):
    raise NotImplementedError("check that DB table matches our fields") # pragma: no cover

  @classmethod
  def insert_mtac(cls, pool_or_cursor, restrict, incfield, fields=(), vals=()):
    "todo: doc what mtac stands for"
    if not isinstance(cls.FIELDS[cls.index(incfield)][1], str):
      raise TypeError('mtac_specialfield_unsupported', 'incfield', incfield)
    if any(not isinstance(cls.FIELDS[cls.index(f)][1], str) for f in restrict):
      raise TypeError('mtac_specialfield_unsupported', 'restrict')
    if len(fields) != len(vals):
      raise ValueError("insert_mtac len(fields) != len(vals)")
    vals = cls.serialize_row(pool_or_cursor, fields, vals)
    where = ' and '.join('%s = %s'%tup for tup in list(restrict.items()))
    mtac = '(select coalesce(max(%s), -1)+1 from %s where %s)'%(incfield, cls.TABLE, where)
    qcols = ','.join([incfield]+list(restrict.keys())+list(fields))
    # todo: are both vals ever empty? if yes this breaks.
    qvals = tuple(restrict.values())+tuple(vals)
    valstring = ','.join(['%s']*len(qvals))
    query = 'insert into %s (%s) values (%s, %s) returning *'%(cls.TABLE, qcols, mtac, valstring)
    return set_options(pool_or_cursor, cls(*commitreturn_or_fetchone(pool_or_cursor, query, qvals)))

  @classmethod
  def pkey_update(cls, pool_or_cursor, pkey_vals, escape_keys, raw_keys=None):
    if not cls.PKEY:
      raise ValueError("can't update %s, no primary key"%cls.TABLE)
    if any(not isinstance(cls.FIELDS[cls.index(f)][1], str) for f in (raw_keys or ())):
      raise TypeError('rawkeys_specialfield_unsupported')
    escape_keys = dict(list(zip(escape_keys, cls.serialize_row(pool_or_cursor, escape_keys, list(escape_keys.values())))))
    pkey = cls.split_pkey()
    raw_keys = raw_keys or {}
    if any(k in pkey for k in list(escape_keys)+list(raw_keys)):
      raise ValueError("pkey field updates not allowed") # todo: why?
    if len(pkey_vals) != len(pkey):
      raise ValueError("len(pkey_vals) %i != len(pkey) %i"%(len(pkey_vals), len(pkey)))
    # todo: if I'm going to allow SpecialField in primary key vals, serialize here
    whereclause = ' and '.join('%s = %%s'%k for k in pkey)
    setclause = ','.join(['%s = %%s'%k for k in escape_keys]+['%s = %s'%tup for tup in list(raw_keys.items())])
    # note: raw_keys could contain %s as well as a lot of other poison
    query = 'update %s set %s where %s'%(cls.TABLE, setclause, whereclause)
    vals = tuple(escape_keys.values())+pkey_vals
    if raw_keys:
      return commitreturn_or_fetchone(pool_or_cursor, query+' returning '+','.join(raw_keys), vals)
    else:
      commit_or_execute(pool_or_cursor, query, vals)
      return None

  def pkey_vals(self):
    if not hasattr(self, 'PKEY') or not self.PKEY:
      raise KeyError("no primary key on %s"%self.TABLE)
    return tuple(map(self.__getitem__, self.split_pkey()))

  def update(self, pool_or_cursor, escape_keys, raw_keys=None):
    pkey_vals = self.pkey_vals()
    # note: do not serialize SpecialField. pkey_update takes care of it.
    rawvals = self.pkey_update(pool_or_cursor, pkey_vals, escape_keys, raw_keys)
    # note: Row has no __setitem__ because this is the only time we want to modify our copy of data (after an update to reflect DB)
    if raw_keys:
      for key, val in zip(raw_keys, rawvals):
        self.values[self.index(key)] = val # this is necessary because raw_keys can contain expressions
      for key in raw_keys:
        if key in self.dirty_cache:
          self.dirty_cache.pop(key)
    escape_keys = dict(list(zip(escape_keys, self.serialize_row(pool_or_cursor, escape_keys, list(escape_keys.values()), for_read=True)))) # ugly; doing it in pkey_update and this
    for key, val in list(escape_keys.items()):
      self.values[self.index(key)] = val
    for key in escape_keys:
      if key in self.dirty_cache:
        self.dirty_cache.pop(key)

  @classmethod
  def updatewhere(cls, pool_or_cursor, where_keys, **update_keys):
    "this doesn't allow raw_keys for now"
    # if cls.JSONFIELDS: raise NotImplementedError # todo: do I need to make the same change for SpecialField?
    if not where_keys or not update_keys:
      raise ValueError
    setclause = ','.join(k+' = %s' for k in update_keys)
    whereclause = ' and '.join(eqexpr(k, v) for k, v in list(where_keys.items()))
    query = 'update %s set %s where %s'%(cls.TABLE, setclause, whereclause)
    vals = tuple(list(update_keys.values())+list(where_keys.values()))
    commit_or_execute(pool_or_cursor, query, vals)

  def delete(self, pool_or_cursor):
    ".. warning:: pgmock doesn't support delete yet, so this isn't tested"
    vals = self.pkey_vals()
    whereclause = ' and '.join('%s = %%s'%k for k in self.split_pkey())
    query = 'delete from %s where %s'%(self.TABLE, whereclause)
    commit_or_execute(pool_or_cursor, query, vals)

  def refkeys(self, fields):
    "returns {ModelClass:list_of_pkey_tuples}. see syncschema.RefKey. Don't use this yet."
    # todo doc: better explanation of what refkeys are and how fields plays in
    ddlist = collections.defaultdict(list)
    if any(f not in self.REFKEYS for f in fields):
      raise ValueError(fields, 'not all in', list(self.REFKEYS.keys()))
    for field in fields:
      refkeys = self.REFKEYS[field]
      for model in refkeys.refmodels:
        ddlist[model].extend(refkeys.pkeys(self, field))
    return ddlist

  def __repr__(self):
    pkey = ' %s'%','.join('%s:%s'%(k, self[k]) for k in self.split_pkey()) if self.PKEY else ''
    return '<%s(pg.Row)%s>'%(self.__class__.__name__, pkey)
