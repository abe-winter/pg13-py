"pg -- postgres wrapper for outlin.es."
# todo: make everything take a pool_or_cursor instead of just a pool (at least make them all check for it)
# todo: don't allow JSONFIELDS to overlap with primary key? think about it. >> is this obsolete with SpecialField?
# todo: add profiling hooks

import sys,psycopg2,psycopg2.extras,logging,psycopg2.pool,contextlib,ujson,functools,collections

def sel(cols,table,where=None):
  'simple query generator'
  # todo(awinter): this isn't used. should it be? else delete.
  return ('select %s from %s where %s'%(cols,table,where)) if where is not None \
    else ('select %s from %s'%(cols,table))

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

class SpecialField(object):
  "helper for fields that are stored as text (or json?) by PG with special ser/des rules in python"
  # todo(awinter): think about collection of serdes classes
  # todo(awinter): inability to do dict of lists means no easy multimap. whatever.
  KNOWN_SERDES=('json','class')
  def __init__(self,pytype,serdes='json'):
    # this is ugly
    if serdes not in self.KNOWN_SERDES: raise TypeError('unk_serdes',serdes)
    if all(hasattr(pytype,meth) for meth in ('ser','des')): serdes='class'
    if serdes=='class' and isinstance(pytype,tuple): raise TypeError("don't mix collection pytype with class-provided serdes")
    self.pytype,self.serdes=pytype,serdes
  def validate(self,pyvar):
    if isinstance(self.pytype,tuple):
      container,item=self.pytype
      if container in (list,set): return isinstance(pyvar,container) and all(isinstance(x,item) for x in pyvar)
      elif container is dict: return isinstance(pyvar,container) and all(isinstance(x,item) for x in pyvar.values())
      else: raise TypeError('unk_container_class',container)
    else: return isinstance(pyvar,self.pytype)
  def validate_raise(self,validate,pyvar):
    if validate and not self.validate(pyvar): raise TypeError(self.pytype,type(pyvar))
  def ser(self,pyvar,validate=True):
    if pyvar is None: raise NullJsonError
    self.validate_raise(validate,pyvar)
    if self.serdes=='json': return ujson.dumps(pyvar)
    elif self.serdes=='class': return pyvar.ser(validate)
    else: raise ValueError('serdes',self.serdes)
  def des(self,underlying,validate=True):
    if self.serdes=='json':
      if underlying is None: raise NullJsonError
      pyvar=ujson.loads(underlying)
      if isinstance(self.pytype,tuple):
        container,item=self.pytype
        # note below: doing a little casting on the container type; whatever. serialized dict could get converted to list of keys
        if container in (list,set): pyvar=container([item(*x) for x in pyvar])
        elif container is dict: pyvar={k:item(*v) for k,v in pyvar.items()}
        else: raise TypeError('unk_container_class',container)
      self.validate_raise(validate,pyvar)
      return pyvar
    elif self.serdes=='class': return self.pytype.des(underlying,validate)
    else: raise ValueError('serdes',self.serdes)
  def sqltype(self): return 'text'

class PgPool(object):
  def __init__(self,dbargs):
    # http://stackoverflow.com/questions/12650048/how-can-i-pool-connections-using-psycopg-and-gevent
    self.pool = psycopg2.pool.ThreadedConnectionPool(5,10,dbargs) # I think that this is safe combined with psycogreen patching
  def select(self,qstring,vals=()):
    with self() as con,con.cursor() as cur:
      cur.execute(qstring,vals)
      for row in cur: yield row # yield stmt has to be in same function as with block to hijack it. todo(awinter): experiment and figure out what that meant.
  def commit(self,qstring,vals=()):
    with self() as con,con.cursor() as cur:
      return cur.execute(qstring,vals)
  def commitreturn(self,qstring,vals=()):
    "commit and return result. This is intended for sql UPDATE ... RETURNING"
    with self() as con,con.cursor() as cur:
      cur.execute(qstring,vals)
      return cur.fetchone()
  def close(self): self.pool.closeall()
  @contextlib.contextmanager
  def __call__(self):
    con = self.pool.getconn()
    try: yield con
    except: raise
    else: con.commit()
    finally: self.pool.putconn(con)

def transform_specialfield((sf,v)): "helper"; return sf.ser(v) if isinstance(sf,SpecialField) else v

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

class Row(object):
  "base class for models"
  # todo: metaclass stuff to check field names on class creation? forbidden column names: returning
  FIELDS = []
  PKEY = ''
  INDEXES = []
  TABLE = ''
  REFKEYS={} # this is used by syncschema. see syncschema.py for usage.
  @classmethod
  def create_indexes(clas,pool_or_cursor):
    "this gets called by create_table, but if you've created an index you can use it to add it (assuming none exist)"
    for index in clas.INDEXES:
      # note: these are specified as either 'field,field,field' or a runnable query. you can put any query you want in there
      query = index if 'create index' in index.lower() else 'create index on %s (%s)'%(clas.TABLE,index)
      pool_or_cursor.commit(query) if isinstance(pool_or_cursor,PgPool) else pool_or_cursor.execute(query)
  @classmethod
  def create_table(clas,pool_or_cursor):
    print clas.__name__,'create_table'
    def mkfield((name,tp)): return name,(tp.sqltype() if isinstance(tp,SpecialField) else tp)
    fields = ','.join(map(' '.join,map(mkfield,clas.FIELDS)))
    base = 'create table if not exists %s (%s'%(clas.TABLE,fields)
    if clas.PKEY: base += ',primary key (%s)'%clas.PKEY
    base += ')'
    pool_or_cursor.commit(base) if isinstance(pool_or_cursor,PgPool) else pool_or_cursor.execute(base)
    clas.create_indexes(pool_or_cursor)
  @classmethod
  def names(class_): return zip(*class_.FIELDS)[0]
  def __eq__(self,other): return all(self[k]==other[k] for k in self.names()) if isinstance(other,type(self)) else False
  def __neq__(self,other): return (not (self==other)) if isinstance(other,type(self)) else False
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
    return self.FIELDS[index][1].des(self.values[index]) if isinstance(self.FIELDS[index][1],SpecialField) else self.values[index]
  @classmethod
  def index(class_,name): return class_.names().index(name)
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
    # need to figure out how to unpack json in a select stmt.
    raise NotImplementedError('todo')
  @classmethod
  def select(clas,pool_or_cursor,**kwargs):
    columns = kwargs.pop('columns','*')
    # todo(awinter): write a test for whether eqexpr matters; figure out pg behavior and apply to pgmock
    query="select %s from %s"%(columns,clas.TABLE)
    if kwargs: query+=' where %s'%' and '.join('%s=%%s'%k for k in kwargs)
    if isinstance(pool_or_cursor,PgPool):
      return pool_or_cursor.select(query,tuple(kwargs.values())) # return the generator
    else: # assume it's a cursor
      pool_or_cursor.execute(query,tuple(kwargs.values()))
      return pool_or_cursor # iterable just like the generator ret by pool.select
  @classmethod
  def select_models(clas,pool_or_cursor,**kwargs):
    "this returns a generator, not a list; careful"
    if 'columns' in kwargs: raise ValueError("don't pass 'columns' to select_models")
    return (clas(*row) for row in clas.select(pool_or_cursor,**kwargs))
  @classmethod
  def selectwhere(clas,pool_or_cursor,userid,qtail,vals=(),needs_and=True):
    "shortcut for passing in the part of the query after 'select * from table where '. yields models, not rows."
    qstring=('select * from %s where userid=%i '%(clas.TABLE,userid))+('and ' if needs_and else '')+qtail
    if isinstance(pool_or_cursor,PgPool):
      retgen=pool_or_cursor.select(qstring,tuple(vals)) # it's a generator (not that that affects performance of big Qs without SS cursors)
    else:
      pool_or_cursor.execute(qstring,tuple(vals))
      retgen=pool_or_cursor
    return (clas(*row) for row in retgen)
  @classmethod
  def select_xiny(clas,pool_or_cursor,userid,field,values):
    "selectwhere shortcut for 'docid in (1,2,3)' type queries"
    if not values: return []
    return clas.selectwhere(pool_or_cursor,userid,'%s in %%s'%field,(tuple(values),))
  @classmethod
  def serialize_row(clas,fieldnames,vals):
    fieldtypes=[clas.FIELDS[clas.index(name)][1] for name in fieldnames]
    return tuple(map(transform_specialfield,zip(fieldtypes,vals)))
  @classmethod
  def insert_all(clas,pool_or_cursor,*vals):
    # note: it would be nice to write this on top of clas.insert, but this returns an object and that has a returning feature. tricky semantics.
    if len(clas.FIELDS)!=len(vals): raise ValueError('fv_len_mismatch',len(clas.FIELDS),len(vals),clas.TABLE)
    vals=clas.serialize_row([f[0] for f in clas.FIELDS],vals)
    query = "insert into %s values (%s)"%(clas.TABLE,','.join(['%s']*len(vals)))
    try: pool_or_cursor.commit(query,vals) if isinstance(pool_or_cursor,PgPool) else pool_or_cursor.execute(query,vals)
    except psycopg2.IntegrityError as e:
      # todo: make sure IntegrityError is always dupe-key
      raise DupeInsert(clas.TABLE,e) # note: pgmock raises DupeInsert directly, so catching this works in caller. (but args are different)
    return clas(*vals)
  @classmethod
  def insert(clas,pool_or_cursor,fields,vals,returning=None):
    if len(fields)!=len(vals): raise ValueError('fv_len_mismatch',len(fields),len(vals),clas.TABLE)
    vals=clas.serialize_row(fields,vals)
    query = "insert into %s (%s) values (%s)"%(clas.TABLE,','.join(fields),','.join(['%s']*len(vals)))
    if returning:
      query += ' returning '+returning
      if isinstance(pool_or_cursor,PgPool): return pool_or_cursor.commitreturn(query,vals)
      else:
        pool_or_cursor.execute(query,vals)
        return pool_or_cursor.fetchone()
    else:
      pool_or_cursor.commit(query,vals) if isinstance(pool_or_cursor,PgPool) else pool_or_cursor.execute(query,vals)
  @classmethod
  def kwinsert(clas,pool_or_cursor,**kwargs):
    "kwargs version of insert"
    returning = kwargs.pop('returning',None)
    fields,vals = zip(*kwargs.items())
    # note: don't do SpecialField resolution here; clas.insert takes care of it
    return clas.insert(pool_or_cursor,fields,vals,returning=returning)
  @classmethod
  def checkdb(clas,pool_or_cursor): raise NotImplementedError("check that DB table matches our fields")
  @classmethod
  def insert_mtac(clas,pool_or_cursor,restrict,incfield,fields=(),vals=()):
    "multitenant auto-increment insert. restrict is a dict of keys and raw values to use for looking up the new incfield id"
    if isinstance(clas.FIELDS[clas.index(incfield)][1],SpecialField): raise TypeError('mtac_specialfield_unsupported','incfield',incfield)
    if any(isinstance(clas.FIELDS[clas.index(f)][1],SpecialField) for f in restrict):
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
    if isinstance(pool_or_cursor,PgPool): return clas(*pool_or_cursor.commitreturn(query,qvals))
    else:
      pool_or_cursor.execute(query,qvals)
      return clas(*pool_or_cursor.fetchone())
  @classmethod
  def pkey_update(clas,pool_or_cursor,pkey_vals,escape_keys,raw_keys=None):
    "raw_keys vs escape_keys -- raw_keys doesn't get escaped, so you can do something like column=column+1"
    if not clas.PKEY: raise ValueError("can't update %s, no primary key"%clas.TABLE)
    if any(isinstance(clas.FIELDS[clas.index(f)][1],SpecialField) for f in (raw_keys or ())): raise TypeError('rawkeys_specialfield_unsupported')
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
    if raw_keys:
      query+=' returning '+','.join(raw_keys)
      if isinstance(pool_or_cursor,PgPool):
        return pool_or_cursor.commitreturn(query,vals)
      else:
        pool_or_cursor.execute(query,vals)
        return pool_or_cursor.fetchone()
    else:
      pool_or_cursor.commit(query,vals) if isinstance(pool_or_cursor,PgPool) else pool_or_cursor.execute(query,vals)
  def pkey_vals(self):
    "get primary key values"
    if not hasattr(self,'PKEY') or not self.PKEY: raise KeyError("no primary key on %s"%self.TABLE)
    return tuple(map(self.__getitem__,self.PKEY.split(',')))
  def update(self,pool_or_cursor,escape_keys,raw_keys=None):
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
    pool_or_cursor.commit(q,vals) if isinstance(pool_or_cursor,PgPool) else pool_or_cursor.execute(q,vals)
  def delete(self,pool_or_cursor):
    vals=self.pkey_vals()
    whereclause=' and '.join('%s=%%s'%k for k in self.PKEY.split(','))
    q='delete from %s where %s'%(self.TABLE,whereclause)
    pool_or_cursor.commit(q,vals) if isinstance(pool_or_cursor,PgPool) else pool_or_cursor.execute(q,vals)
  def clientmodel(self):
    "use the class's CLIENTFIELDS attribute to create a dictionary the client can read"
    raise NotImplementedError # todo delete: this is only used in dead pinboard code and a test
    return {k:self[k] for k in self.CLIENTFIELDS}
  def refkeys(self,fields):
    "returns {ModelClass:list_of_pkey_tuples}. see syncschema.RefKey."
    # todo doc: better explanation of what refkeys are and how fields plays in
    dd=collections.defaultdict(list)
    if any(f not in self.REFKEYS for f in fields): raise ValueError(fields,'not all in',self.REFKEYS.keys())
    for f in fields: dd[self.REFKEYS[f].refmodel].extend(self.REFKEYS[f].pkeys(self,f))
    return dd
  def __repr__(self):
    pkey = ' %s'%','.join('%s:%s'%(k,self[k]) for k in self.PKEY.split(',')) if self.PKEY else ''
    return '<%s(pg.Row)%s>'%(self.__class__.__name__,pkey)
