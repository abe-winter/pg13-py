"msgpack-enabled version-aware model layer for redis. also pubsub client."

import msgpack,socket

class RedisModel(object):
  "don't use overlapping names in KEY/VALUE"
  NAMESPACE=None
  VERSION=None
  KEY=None
  VALUE=None
  TTL=None # this can stay None in derived class if you don't want a TTL
  @classmethod
  def type_check(clas,types,vals,errname):
    if len(types)!=len(vals): raise TypeError('length mismatch for %s.%s (got %i want %i)'%(clas.__name__,errname,len(vals),len(types)))
    for i,(v,t) in enumerate(zip(vals, types)):
      if not isinstance(v,t): raise TypeError('type_mismatch', clas.__name__, errname, {'index':i, 'want':t, 'got':type(v)})
  @classmethod
  def make_key(clas,*keyvals):
    if any(x is None for x in (clas.NAMESPACE,clas.VERSION,clas.KEY)): raise TypeError('null meta-information in class')
    clas.type_check(zip(*clas.KEY)[1],keyvals,'KEY')
    return msgpack.dumps((clas.NAMESPACE,clas.VERSION)+keyvals)
  @classmethod
  def make_val(clas,*vals):
    "this doesn't need to be a classmethod -- it can be a normal instance method"
    if clas.VALUE is None: raise TypeError('null meta-information in class for VALUE')
    clas.type_check(zip(*clas.VALUE)[1],vals,'VALUE')
    return msgpack.dumps(vals)
  @classmethod
  def get(clas,con,*keyvals):
    vals=con.get(clas.make_key(*keyvals))
    return None if vals is None else clas(list(keyvals),msgpack.loads(vals))
  @classmethod
  def incrby(clas,con,n,*keyvals):
    k=clas.make_key(*keyvals)
    ret=con.incrby(k,n)
    if clas.TTL: con.expire(k,clas.TTL) # todo: batch this once stubredis supports batching
    return ret
  @classmethod
  def delete(clas,con,*keyvals): return con.delete(clas.make_key(*keyvals))
  @classmethod
  def des(clas,keyblob,valblob):
    "deserialize. translate publish message, basically"
    raise NotImplementedError("don't use tuples, it breaks __eq__. this function probably isn't used in real life")
    raw_keyvals=msgpack.loads(keyblob)
    (namespace,version),keyvals=raw_keyvals[:2],raw_keyvals[2:]
    if namespace!=clas.NAMESPACE or version!=clas.VERSION:
      raise TypeError('des_mismatch got %s want %s'%((namespace,version),(clas.NAMESPACE,clas.VERSION)))
    vals=tuple(msgpack.loads(valblob))
    clas.type_check(zip(*clas.KEY)[1],keyvals,'KEY')
    clas.type_check(zip(*clas.VALUE)[1],vals,'VALUE')
    return clas(tuple(keyvals),vals)
  def __init__(self,keyvals,vals): self.keyvals,self.vals=keyvals,vals
  def kv(self): return self.make_key(*self.keyvals),self.make_val(*self.vals)
  def save(self,con):
    k,v=self.kv()
    con.set(k,v,ex=self.TTL)
  def saveget(self,con):
    "save, return old value. todo: make the expire() atomic with a redis script"
    k,v=self.kv()
    oldv=con.getset(k,v)
    if self.TTL is not None: con.expire(k,self.TTL)
    return None if oldv is None else msgpack.loads(oldv)
  def pub(self,con):
    "careful -- save and pub aren't the same"
    k,v=self.kv()
    con.publish(k,v)
  def save_and_pub(self,con): self.save(con); self.pub(con) # todo: atomic
  def expire(self,con): con.expire(self.make_key(*self.keyvals), self.TTL)
  def ttl(self,con): "remaining time-to-live in seconds"; return con.ttl(self.make_key(*self.keyvals))
  def __repr__(self):
    keys=','.join('%s:%r'%(nm,v) for (nm,tp),v in zip(self.KEY,self.keyvals))
    vals=','.join('%s:%r'%(nm,v) for (nm,tp),v in zip(self.VALUE,self.vals))
    return '<RedisModel %s KEY %s VALUE %s>'%(self.__class__.__name__,keys,vals)
  def __getitem__(self,field):
    "this can definitely raise a KeyError, probably and IndexError under weird circumstances"
    keynames = zip(*self.KEY)[0]
    valnames = zip(*self.VALUE)[0]
    if field in keynames: return self.keyvals[keynames.index(field)]
    elif field in valnames: return self.vals[valnames.index(field)]
    else: raise KeyError(field)
  def __eq__(self, other):
    return isinstance(other,type(self)) and self.keyvals == other.keyvals and self.vals == other.vals

class PubsubError(IOError): "hmm"
class PubsubDisco(PubsubError): "hmm"
class RedisSimplePubsub:
  "pubsub client with timeout support (unlike redis-py)"
  NTRIES=2 # for reconnect
  def __init__(self,host,port,timeout):
    "timeout float seconds"
    self.host,self.port,self.timeout=host,port,timeout
    self.sock,self.buf,self.reset,self.sublist=None,'',False,set()
  def getcon(self):
    if self.sock is None:
      print 'getcon todo: lock and recheck'
      self.sock=socket.create_connection((self.host,self.port))
      if self.timeout is not None: self.sock.settimeout(self.timeout) # float seconds
      self.onrecon()
    return self.sock
  def send(self,payload):
    for i in range(self.NTRIES):
      try: self.getcon().send(payload)
      except socket.error as e:
        logging.error()
        self.sock=None
        self.buf=''
        self.reset=True
        logging.error('RedisSimplePubsub.send %s %s'%(e.__class__.__name__,str(e)))
      else: break
  def onrecon(self): "this gets called on reconnect"; map(self.subscribe,self.sublist)
  def subscribe(self,k):
    self.sublist.add(k)
    self.getcon().send('*2\r\n'+bulkstring('SUBSCRIBE')+bulkstring(k))
  def unsubscribe(self,k):
    self.sublist.remove(k) # todo: report the error but don't die
    self.getcon().send('*2\r\n'+bulkstring('UNSUBSCRIBE')+bulkstring(k))
  def wait(self):
    "wait for a message, respecting timeout"
    data=self.getcon().recv(256) # this can raise socket.timeout
    if not data: raise PubsubDisco
    if self.reset:
      self.reset=False # i.e. ack it. reset is used to tell the wait-thread there was a reconnect (though it's plausible that this never happens)
      raise PubsubDisco
    self.buf+=data
    msg,self.buf=complete_message(self.buf)
    return msg
