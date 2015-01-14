"this includes tests for syncmessage"

import pytest,ujson,binascii
from pg13 import syncschema,syncmessage,pg,pgmock,diff,misc

def test_vdstring():
  # Syncable subclasses should test: apply, generate, validate, ser, des, create, version
  from pg13.syncschema import VDString
  from pg13.diff import Delta
  vds=VDString().apply(0,[Delta(0,0,'whatsup')],binascii.crc32('whatsup')) # apply
  with pytest.raises(syncschema.BadBaseV): vds.apply(0,[Delta(0,0,'')],binascii.crc32('whatsup')) # apply badbase
  assert vds.generate()=='whatsup' # generate
  assert vds.version()==1 # version
  assert VDString.create('hello').version()==1 # create

def test_vdstring_serdes():
  from pg13.syncschema import VDString
  (utc,deltas,crc),=ujson.loads(VDString.create('hello').ser()) # ser
  assert deltas==[[0,0,'hello']]
  vds=VDString.des(VDString.create('whatsup').ser()) # des
  assert vds.version()==1 and vds.generate()=='whatsup'
  with pytest.raises(StandardError): VDString.des('"garbage"') # des fail

def test_vhstring():
  # see test_vdstring and test_vdlist for more comments about what's being tested here
  from pg13.syncschema import VHString
  vhs=VHString().apply(0,'hello')
  with pytest.raises(syncschema.BadBaseV): vhs.apply(0,'fail')
  assert vhs.generate()=='hello'
  assert vhs.version()==1
  (utc,val,crc),=ujson.loads(vhs.ser())
  assert isinstance(utc,int) and val=='hello' and crc is None
  assert VHString.create('hello2').generate()=='hello2'

def test_vhstring_serdes():
  from pg13.syncschema import VHString
  vhs=VHString.des(VHString.create('hello').ser())
  assert vhs.generate()=='hello' and vhs.version()==1
  with pytest.raises(StandardError): VHString.des('fail')
  with pytest.raises(TypeError): VHString.des(ujson.dumps([0,[],None]))

def test_vdlist():
  from pg13.syncschema import VDList
  from pg13.diff import Delta
  vl=VDList().apply(0,[Delta(0,0,['a','b','c']),Delta(3,3,['d']),Delta(0,1,['e'])]) # test apply
  with pytest.raises(syncschema.BadBaseV): vl.apply(0,[]) # test apply bad base
  assert ['e','b','c','d']==vl.generate() # test generate
  assert vl.version()==1 # test version()
  assert len(VDList.create([1,2,3]).generate())==3

def test_vdlist_serdes():
  from pg13.syncschema import VDList
  assert [1,2,3]==VDList.des(VDList.create([1,2,3]).ser()).generate()

def test_empties():
  assert []==syncschema.VDList().generate()
  assert ''==syncschema.VHString().generate()
  assert ''==syncschema.VDString().generate()

def test_schema_unicode():
  "make sure the V** classes can handle unicode"
  hearts=u'\u2661'*3
  assert [hearts]==syncschema.VDList.create([hearts]).generate()
  vds=syncschema.VDString().apply(0,[diff.Delta(0,0,hearts)],binascii.crc32(hearts.encode('utf-8')))
  assert vds.generate()==hearts
  assert hearts==syncschema.VDString.create(hearts).generate()
  assert hearts==syncschema.VHString.create(hearts).generate()

def test_translate_update():
  translated=syncmessage.translate_update({
    ujson.dumps(['nombre',['k1','k2'],'field']):{
      'vbase':0,
      'vnew':1,
      'mtime':0,
      'deltas':[{'slice':{'a':0,'b':0},'replace':['a','b']}],
      'crc':None,
    },
    ujson.dumps(['nombre',['k1','k2'],'f2']):['checkstale',0],
  })
  assert all(isinstance(k,syncmessage.FieldKey) for k in translated)
  assert all(isinstance(v,(syncmessage.SerialDiff,syncmessage.CheckStale)) for v in translated.values())
  assert all(isinstance(d,diff.Delta) for d in next(v for k,v in translated.items() if k.field=='field').deltas)
def test_translate_check():
  (k,v),=syncmessage.translate_check({ujson.dumps(['nombre',['k1','k2'],'field']):0,}).items()
  assert isinstance(k,syncmessage.FieldKey) and isinstance(v,int)

class Ref(pg.Row):
  "Simple below refers to this"
  FIELDS=[('userid','int'),('docid','text'),('tag','text'),('xfactor',pg.SpecialField(syncschema.VDList,'class'))]
  PKEY='userid,docid,tag'
  TABLE='ref'

class Simple(pg.Row):
  "simple model for testing do_*"
  FIELDS=[('userid','int'),('docid','text'),('tags',pg.SpecialField(syncschema.VDList,'class'))]
  PKEY='userid,docid'
  TABLE='simple'
  REFKEYS={'tags':syncschema.RefKey(Ref,['userid','docid',None])}

MODELGB=misc.GetterBy([
  syncmessage.ModelInfo('simple',Simple,None),
  syncmessage.ModelInfo('ref',Ref,None),
])

def test_simple():
  "make sure it's possible to construct one of these, load-store with the DB, and run diff ops on it"
  pool=pgmock.PgPoolMock()
  Simple.create_table(pool)
  model=Simple.insert_all(pool,0,'1',syncschema.VDList())
  assert model['tags'].generate()==[]
  model=Simple.pkey_get(pool,0,'1')
  assert model['tags'].generate()==[]
  model.update(pool,{'tags':model['tags'].apply(0,[diff.Delta(0,0,['a'])])})
  assert model['tags'].generate()==['a']
  assert Simple.pkey_get(pool,0,'1')['tags'].generate()==['a']

def test_refkey():
  model=Simple(0,'1',syncschema.VDList.create(['a','b','c']).ser())
  assert model.refkeys(['tags'])=={Ref:[(0,'1','a'),(0,'1','b'),(0,'1','c')]}

def test_amc():
  pool=pgmock.PgPoolMock()
  Simple.create_table(pool)
  request=syncmessage.translate_check({ujson.dumps(['simple',[0,'1'],'tags']):0})
  syncmessage.add_missing_children(
    {('simple',(0,'1')):Simple.insert_all(pool,0,'1',syncschema.VDList.create(['a']))},
    request,
    {'simple':['tags']},
    MODELGB
  )
  assert request[syncmessage.FieldKey('ref',(0,'1','a'),'xfactor')] is None

def mkdict(val,pkey=(0,'1'),nombre='simple',field='tags'):
  "helper for do_*"
  return {syncmessage.FieldKey(nombre,pkey,field):val}

def update_helper(request):
  # def do_update(pool,request,models):
  pool=pgmock.PgPoolMock()
  Simple.create_table(pool)
  return syncmessage.do_update(pool,
    request,
    {('simple',(0,'1')):Simple.insert_all(pool,0,'1',syncschema.VDList.create([]))}
  )
def test_do_update():
  # SerialDiff=collections.namedtuple('SerialDiff','vbase vnew mtime deltas crc')
  assert update_helper(mkdict(syncmessage.SerialDiff(1,2,0,[
    diff.Delta(0,0,['a','b'])
  ],None)))==mkdict(['ok',2]) # todo: also test that the right thing gets stored in the DB
  assert update_helper(mkdict(syncmessage.SerialDiff(2,2,0,[],None)))==mkdict(['nobase!',1])
  assert update_helper(mkdict(syncmessage.SerialDiff(0,1,0,[],None)))==mkdict(['merge!',1,[]])

def test_update_checkstale():
  assert [['chkstale',1,[]]]==update_helper(syncmessage.translate_update({
    ujson.dumps(['simple',[0,'1'],'tags']):['checkstale',0],
  })).values()
  # and also make sure missing doesn't kill it
  assert [['chkstale',None,None]]==update_helper(syncmessage.translate_update({
    ujson.dumps(['simple',[0,'2'],'tags']):['checkstale',0],
  })).values()

def check_helper(request):
  pool=pgmock.PgPoolMock()
  Simple.create_table(pool)
  return syncmessage.do_check(pool,
    request,
    {('simple',(0,'1')):Simple.insert_all(pool,0,'1',syncschema.VDList.create([]))},
    {},
    MODELGB
  )
def test_do_check():
  assert check_helper(mkdict(1))==mkdict(['ok',1]) # ok case
  assert check_helper(mkdict(2))==mkdict(['upload',1]) # upload case
  assert check_helper(mkdict(None))==mkdict(['here',1,[]]) # load case
  assert check_helper(mkdict(0))==mkdict(['here',1,[]]) # newer case

def test_missing_fields():
  assert [['?field']]==check_helper(mkdict(0,field='whatever')).values()
  assert [['?field']]==update_helper(mkdict(0,field='whatever')).values()

def test_check_missing():
  "figure out what to do when model lookup fails"
  assert [['missing']]==check_helper(mkdict(0,pkey=(0,'2'))).values()
