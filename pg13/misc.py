"misc whatever"

import time,os,collections,sys

def utcnow(): return int(time.time())

def tbframes(tb):
  'unwind traceback tb_next structure to array'
  frames=[tb.tb_frame]
  while tb.tb_next: tb=tb.tb_next; frames.append(tb.tb_frame)
  return frames
def tbfuncs(frames):
  'this takes the frames array returned by tbframes'
  return ['%s:%s:%s'%(os.path.split(f.f_code.co_filename)[-1],f.f_code.co_name,f.f_lineno) for f in frames]
def trace(): return tbfuncs(tbframes(sys.exc_info()[2]))

# warning: EnvBundle is too specific to the applications that spawned pg13. ok to release to OSS, but make a more general way to pass stuff around.
EnvBundle=collections.namedtuple('EnvBundle','pool redis sesh userid chanid apicon') # environment bundle; convenient capsule for passing this stuff around

class GetterBy(list):
  "sort of like a read-only dict for namedtuple. inherits from list so it's iterable"
  def __init__(self,tups): super(GetterBy,self).__init__(tups)
  def __getitem__(self,(key,value)): return next(x for x in self if getattr(x,key)==value)
  def __contains__(self,(key,value)): return any(getattr(x,key)==value for x in self)
