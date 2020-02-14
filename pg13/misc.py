"misc whatever"

import time, os, collections, sys, functools, itertools

def utcnow():
  return int(time.time())

def tbframes(traceback):
  'unwind traceback tb_next structure to array'
  frames = [traceback.tb_frame]
  while traceback.tb_next:
    traceback = traceback.tb_next
    frames.append(traceback.tb_frame)
  return frames
def tbfuncs(frames):
  'this takes the frames array returned by tbframes'
  return ['%s:%s:%s' % (os.path.split(f.f_code.co_filename)[-1], f.f_code.co_name, f.f_lineno) for f in frames]
def trace():
  return tbfuncs(tbframes(sys.exc_info()[2]))

def key_from_pair(pair):
  "helper for multimap"
  return pair[0]

def multimap(kv_pairs):
  # note: sort is on just key, not k + v, because sorting on both would require sortable value type
  return {
    key: [v for _, v in pairs]
    for key, pairs in itertools.groupby(sorted(kv_pairs, key=key_from_pair), key_from_pair)
  }

# warning: EnvBundle is too specific to the applications that spawned pg13. ok to release to OSS, but make a more general way to pass stuff around.
EnvBundle = collections.namedtuple('EnvBundle', 'pool redis sesh userid chanid apicon') # environment bundle; convenient capsule for passing this stuff around

class CallOnceError(Exception):
  pass

def meth_once(func):
  "call once for member function (i.e. takes self as first arg)"
  attr = '__meth_once_' + func.__name__
  @functools.wraps(func)
  def wrapper(self, *args, **kwargs):
    if hasattr(self, attr):
      raise CallOnceError(func.__name__)
    setattr(self, attr, True)
    return func(self, *args, **kwargs)
  return wrapper
