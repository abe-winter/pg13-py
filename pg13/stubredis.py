"redis-compatible server for doing windows testing (i.e. redis doesn't build on windows)."

import redis,msgpack,time,os,SocketServer,argparse,socket,threading,logging
from collections import defaultdict
from . import redismodel

REDIS_OK='+OK\r\n'

def bulkstring(v):
  if v is None: return "$-1\r\n"
  elif isinstance(v,str): return '$%i\r\n%s\r\n'%(len(v),v) # unicode isn't a concern: anything from socket.read() will be a byte string
  else: raise TypeError(type(v))

class RedisState:
  "mini fake redis server for windows testing. glorified dict with TTL and pubsub channels. stores socks for pubsub, which violates design."
  SOCK_COMMANDS=('SUBSCRIBE')
  def __init__(self):
    self.keys,self.ttl={},{}
    self.pubsub=defaultdict(list)
    self.sock2sub=defaultdict(list)
    self.lock=threading.Lock()
  def process_message(self,msg,sock):
    "serialize and deserialize"
    command=msg[0]
    try: f={'GET':self.get,'SET':self.set,'SUBSCRIBE':self.sub,'PUBLISH':self.pub,
      'PING':self.ping,'GETSET':self.getset,'EXPIRE':self.expire,'DEL':self.delete}[command]
    except KeyError: print msg; raise
    args=msg[1:]
    try: return f(sock,*args) if command in self.SOCK_COMMANDS else f(*args)
    except Exception as e:
      print e
      print msg
      return '-ERROR\r\n'
  def get(self,k):
    if k in self.ttl and self.ttl[k]<time.time(): del self.ttl[k]; del self.keys[k]
    return bulkstring(self.keys.get(k))
  def set(self,k,v,*args):
    self.keys[k]=v
    self.ttl.pop(k,None)
    if args:
      if len(args)%2!=0: raise ValueError(args)
      kwargs=dict(zip(args[::2],args[1::2]))
      for argk,v in kwargs.items():
        if argk=='EX': self.ttl[k]=time.time()+int(v)
        else: raise NotImplementedError('unk set kwargs in %r'%kwargs)
    return REDIS_OK
  def getset(self,k,v):
    ret=bulkstring(self.keys.get(k))
    self.set(k,v)
    return ret
  def pub(self,k,v):
    # note: this is a long locked operation. might want to have a granular locking list like the SOCK_COMMANDS list
    if k in self.pubsub:
      msg='*3\r\n'+bulkstring('message')+bulkstring(k)+bulkstring(v)
      for sock in self.pubsub[k][:]: # copy so we can delete while iterating
        try: sock.send(msg); print 'sent %r to sock %s'%(msg,sock)
        except socket.error: self.pubsub[k].remove(sock); print 'removed sock %s'%sock
    return REDIS_OK # is this right?
  def sub(self,sock,k):
    print 'todo: I think SUBSCRIBE can have more than one key'
    if k in self.sock2sub[sock]: raise NotImplementedError # return error for double-subscribe
    self.pubsub[k].append(sock)
    self.sock2sub[sock].append(k)
    return '*3\r\n'+bulkstring('subscribe')+bulkstring(k)+(':%i\r\n'%len(self.sock2sub[sock]))
  def info(self,*args): raise NotImplementedError
  def select(self,*args): raise NotImplementedError
  def ping(self): return '+PONG\r\n'
  def expire(self,k,seconds_raw):
    try: seconds=int(seconds_raw)
    except ValueError: return ':0\r\n'
    if k in self.keys: # todo: return 0 for already-expired keys. todo: tryexpire() function everywhere, run it on all keys affected by op.
      self.ttl[k]=time.time()+seconds
      return ':1\r\n'
    else: return ':0\r\n'
  def delete(self,*keys):
    n=sum(self.keys.pop(k,None) is not None for k in keys)
    for k in keys: self.ttl.pop(k,None)
    return ':%i\r\n'%n

def complete_message(buf):
  "returns msg,buf_remaining or None,buf"
  # todo: read dollar-length for strings; I dont think I can blindly trust newlines. learn about escaping
  # note: all the length checks are +1 over what I need because I'm asking for *complete* lines.
  lines=buf.split('\r\n')
  if len(lines)<=1: return None,buf
  nargs_raw=lines.pop(0)
  assert nargs_raw[0]=='*'
  nargs=int(nargs_raw[1:])
  args=[]
  while len(lines)>=2: # 2 because if there isn't at least a blank at the end, we're missing a terminator
    if lines[0][0]=='+': args.append(lines.pop(0))
    elif lines[0][0]==':': args.append(int(lines.pop(0)[1:]))
    elif lines[0][0]=='$':
      if len(lines)<3: return None,buf
      slen,s=int(lines[0][1:]),lines[1]
      if slen!=len(s): raise ValueError('length mismatch %s %r'%(slen,s)) # probably an escaping issue
      lines=lines[2:]
      args.append(s)
    else: raise ValueError('expected initial code in %r'%lines)
  if len(args)==nargs: return args,'\r\n'.join(lines)
  else: return None,buf

class RedisHandler:
  "handler object to pass to TCPServer ctor.\
  Manages deserialization but RedisState handles serialization.\
  (I'm not sure if there's a 1:1 map of python type to redis reply format.\
    Maybe I can with knowledge of the command)."
  def __init__(self,sock,addr,server,verbose=True):
    if verbose: print 'RedisHandler for client %s:%i'%addr
    buf=''
    sock.settimeout(0.5)
    while not server.stop_looping:
      try: data=sock.recv(1000)
      except socket.timeout: continue
      except socket.error as e:
        print 'breaking on %s'%e
        break
      if not data: print 'breaking on null recv'; break
      buf+=data
      msg,buf=complete_message(buf)
      if msg:
        with server.redis_state.lock: raw_reply=server.redis_state.process_message(msg,sock)
        sock.send(raw_reply)

class SubNotifyThread(threading.Thread):
  "used for repl mode"
  def __init__(self,host,port,timeout=0.1):
    self.dead,self.commands=False,[]
    self.pubsub=redismodel.RedisSimplePubsub(host,port,timeout)
    super(SubNotifyThread,self).__init__()
  def run(self):
    while not self.dead:
      commands,self.commands=self.commands,[]
      for command,key in commands:
        dict(sub=self.pubsub.subscribe,unsub=self.pubsub.unsubscribe)[command](key)
      try: msg=self.pubsub.wait()
      except socket.timeout: pass
      except PubsubDisco: print 'todo: handle disco'; raise
      else: print 'todo: msg',msg
  def subscribe(self,k): self.commands.append(('sub',k))
  def unsubscribe(self,k): self.commands.append(('unsub',k))

def repl(host,port):
  "todo: replace this with real redis client"
  client=redis.StrictRedis(host,port,socket_timeout=0.1) # timeout is for pubsub -- it shares the connection_pool
  pubsub=client.pubsub()
  snt=SubNotifyThread(host,port); snt.start()
  while 1:
    try: command=raw_input('command> ') # this has up/down command history on windows. How?
    except KeyboardInterrupt: print 'ctrl+c'; break
    if command=='exit': break
    if not command: continue
    toks=command.split()
    try: f=dict(get=client.get,set=client.set,pub=client.publish,sub=snt.subscribe,unsub=snt.unsubscribe,ping=client.ping)[toks[0]]
    except KeyError: print 'unk_command %s'%toks[0]; continue
    try: print f(*toks[1:])
    except Exception as e: print 'error %s %s'%(e.__class__.__name__,str(e))
  snt.dead=True
  print 'waiting for pubsub thread'
  snt.join() # todo: just kill it

def servermode(host,port):
  tcps=SocketServer.ThreadingTCPServer((host,port),RedisHandler)
  tcps.redis_state=RedisState()
  tcps.stop_looping=False
  print 'starting server'
  try: tcps.serve_forever()
  except KeyboardInterrupt: print 'ctrl+c'
  tcps.stop_looping=True

def main():
  p=argparse.ArgumentParser()
  p.add_argument('mode',choices='server repl test'.split())
  p.add_argument('port',type=int,help='port to serve on OR port to connect to (depending on mode)')
  p.add_argument('--host',default='127.0.0.1')
  args=p.parse_args()
  if args.mode=='server': servermode(args.host,args.port)
  elif args.mode=='repl': repl(args.host,args.port)
  elif args.mode=='test': raise NotImplementedError
  else: raise ValueError('bad mode %s'%args.mode)

if __name__=='__main__': main()
