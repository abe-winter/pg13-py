"pg.Row tests using pgmock"

import pytest,collections,ujson
from pg13 import pg,pgmock,misc

class Model(pg.Row):
  FIELDS = [('userid','int'),('id2','int'),('content','text'),('arr',pg.SpecialField(list))]
  PKEY = 'userid,id2'
  TABLE = 'model'
  @pg.dirty('content')
  def content_len(self): return len(self['content'])

USERID=1

def prepmock(*schemas):
  "helper; create a mock environment"
  pool=pgmock.PgPoolMock()
  for s in schemas: s.create_table(pool)
  return misc.EnvBundle(pool,None,None,USERID,None,None)

@pytest.mark.xfail
def test_create_indexes(): raise NotImplementedError # ignoring; no support for indexes in pgmock
def test_create_table():
  ebuns=prepmock(Model)
  assert 4==len(ebuns.pool.tables['model'].fields)
  assert ['userid','id2']==ebuns.pool.tables['model'].pkey
  with pytest.raises(ValueError) as e: Model.create_table(ebuns.pool)
  assert e.value.args==('table_exists','model')
def test_get(): assert 'a'==Model(0,1,'a','[]')['content']
def test_get_jsonfields(): assert []==Model(0,1,'a','[]')['arr']
def test_index(): assert all(i==Model.index(name) for i,(name,tp) in enumerate(Model.FIELDS))

def populate(n=3,nusers=2):
  "helper"
  ebuns=prepmock(Model)
  for i in range(nusers):
    for j in range(n): Model.insert_all(ebuns.pool,i,j,'a',[])
  return ebuns

def test_pkey_get():
  ebuns=populate(3)
  Model.pkey_get(ebuns.pool,1,0)
  with pytest.raises(pg.Missing): Model.pkey_get(ebuns.pool,3,0)
def test_select():
  ebuns=populate(3)
  assert [[0,1,'a','[]'],[1,1,'a','[]']]==list(Model.select(ebuns.pool,id2=1))
def test_row_eq():
  assert Model(1,0,'a','[]')==Model(1,0,'a','[]')
  assert Model(1,0,'a','[]')!=Model(1,2,'a','[]')
def test_select_models():
  ebuns=populate(3)
  assert [Model(0,1,'a','[]'),Model(1,1,'a','[]')]==list(Model.select_models(ebuns.pool,id2=1))
  assert 6==len(list(Model.select_models(ebuns.pool))) # select all used to not work
def test_selectwhere():
  ebuns=populate(3)
  assert [Model(0,2,'a','[]')]==list(Model.selectwhere(ebuns.pool,0,'id2>%s',(1,)))
@pytest.mark.xfail
def test_insert():
  ebuns=prepmock(Model)
  Model.insert(ebuns.pool,['userid','id2'],[0,1])
  with pytest.raises(pgmock.DuplicateError):
    Model.insert(ebuns.pool,['userid','id2'],[0,1])
  # todo: make sure it handles JSONFIELDS correctly
  print Model.insert(ebuns.pool,['userid','id2'],[0,2],'id2') # todo: figure out multi-column returning
  raise NotImplementedError
def test_insert_all():
  ebuns=prepmock(Model)
  # note: see note on null-handling trickiness in SpecialField.des
  with pytest.raises(pg.NullJsonError): Model.insert_all(ebuns.pool,0,0,'a',None)['arr']
  assert []==Model.insert_all(ebuns.pool,0,0,'a',[])['arr']
  assert [[0,0,'a','[]']]==ebuns.pool.tables['model'].rows
  assert []==Model.pkey_get(ebuns.pool,0,0)['arr']
def test_kwinsert():
  ebuns=prepmock(Model)
  Model.kwinsert(ebuns.pool,userid=0,id2=1,arr=[])
  assert ebuns.pool.tables['model'].rows==[[0,1,None,'[]']]
  # todo: test 'returning' feature
@pytest.mark.xfail
def test_checkdb(): raise NotImplementedError # todo: find out what PG supports and implement it for pgmock
def test_insert_mtac():
  ebuns=prepmock(Model)
  # todo: edit pg.Row.insert_mtac docs re raw_value -- this works with int but not 'userid':'0'
  assert 0==Model.insert_mtac(ebuns.pool,{'userid':0},'id2',('content',),('a',))['id2']
  assert 1==Model.insert_mtac(ebuns.pool,{'userid':0},'id2',('content',),('a',))['id2']
def test_pkey_update():
  # todo(awinter): raw_keys
  # todo(awinter): jsonfields and raw_keys vs escape_keys
  ebuns=populate(2)
  Model.pkey_update(ebuns.pool,(0,0),{'content':'whatever'})
  assert [0,0,'whatever','[]']==Model.select(ebuns.pool,userid=0,id2=0)[0]
def test_pkey_vals(): assert (10,20)==Model(10,20,'a','[]').pkey_vals()
def test_update():
  # def update(self,pool_or_cursor,escape_keys,raw_keys=None):
  ebuns=populate(2)
  Model(0,0,'a','[]').update(ebuns.pool,{'content':'whatever'})
  assert [0,0,'whatever','[]']==Model.select(ebuns.pool,userid=0,id2=0)[0]
  m=Model(0,0,'a','[]')
  m.update(ebuns.pool,{'arr':[1,2]})
  assert [1,2]==m['arr'],"update isn't re-serializing updated values"
  # todo(awinter): raw keys, jsonfields
def test_update_rawkeys():
  "there was a query construction bug for rawkeys without escapekeys"
  ebuns=populate(1,1)
  m=Model(0,0,'a','[]')
  m.update(ebuns.pool,{},{'content':"'whatever'"})
  assert m['content']=='whatever'
  assert ebuns.pool.tables['model'].rows==[[0,0,'whatever','[]']]
def test_updatewhere():
  ebuns=populate(2)
  Model.updatewhere(ebuns.pool,{'userid':1},content='userid_1')
  for userid,_,content,_ in ebuns.pool.tables['model'].rows:
    assert content==('userid_1' if userid==1 else 'a')

def test_delete():
  ebuns=populate(2,2)
  Model(0,0,'a','[]').delete(ebuns.pool)
  assert 3==len(ebuns.pool.tables['model'].rows)
  assert not Model.select(ebuns.pool,userid=0,id2=0)

@pytest.mark.xfail
def test_clientmodel():
  "this probably goes away with sync schemas"
  raise NotImplementedError

class SerDesClass:
  "helper for test_specialfield_*"
  def __init__(self,x): self.x=x
  def ser(self,validate=True): return str(self.x)
  @staticmethod
  def des(blob,validate=True): return int(blob)
nt=collections.namedtuple('nt','a b')
SF_TESTS=[
    # single pytype
    [(dict,),{'hello':'there'}],
    # tuple pytype
    [((list,nt),),[nt(1,2),nt(3,4)]],
    [((dict,nt),),dict(a=nt(1,2),b=nt(2,3))],
    [((set,nt),),{nt(1,2),nt(3,4)}],
    # serdes class
    [(SerDesClass,'class'),SerDesClass(5)],
]

def test_specialfield_ser():
  from pg13.pg import SpecialField
  for sfargs,val in SF_TESTS:
    if sfargs[1:2]==('class',): assert val.ser(None)==SpecialField(*sfargs).ser(val)
    else: assert ujson.dumps(val)==SpecialField(*sfargs).ser(val)

def test_specialfield_des():
  from pg13.pg import SpecialField
  # note: namedtuples evaluate equal to tuples, but json ser/deses to list. so if there are tuples in there, this is probably working.
  for sfargs,val in SF_TESTS:
    if sfargs[1:2]==('class',): assert sfargs[0].des(val.ser(),None)==SpecialField(*sfargs).des(val.ser())
    else: assert val==SpecialField(*sfargs).des(ujson.dumps(val))

def test_dirtycache():
  ebuns=prepmock(Model)
  m=Model.insert_all(ebuns.pool,0,0,'a',[])
  assert 1==m.content_len()
  # 1. bypassing normal update procedure gets stale value
  m.values[m.index('content')]='abc'
  assert 1==m.content_len()
  # 2. escape_keys
  m.update(ebuns.pool,{'content':'abc'})
  assert 3==m.content_len()
  # 3. raw_keys
  m.update(ebuns.pool,{},{'content':"'abcd'"})
  assert 4==m.content_len()

@pytest.mark.xfail
def test_specialfield_nullhandling():
  raise NotImplementedError # what's this supposed to do?

@pytest.mark.xfail
def test_refkeys():
  # todo(PREOPENSOURCE): write this. for now, this is handled by test_syncapi.test_doc_refkeys in the oes codebase.
  raise NotImplementedError
