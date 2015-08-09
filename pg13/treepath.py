"""treepath.py -- storage for tree-structured parse results.
supports 'paths', tuples that describe how to index the tree.
paths can be used to get and set.
todo: this probably exists somewhere else so use a public library instead of roll-your-own.
"""

class PathTree(object):
  "'tree path' is implemented here (i.e. square brackets for get-set)"
  def child(self,index):
    "helper for __getitem__/__setitem__"
    if isinstance(index,tuple):
      attr,i = index
      return getattr(self,attr)[i]
    else: return getattr(self,index)
  
  def check_i(self,i):
    "helper"
    if not isinstance(i,tuple): raise TypeError('want:tuple',type(i))
  
  def __getitem__(self,i):
    self.check_i(i)
    if len(i)==0: return self
    elif len(i)==1: return self.child(i[0])
    else: return self.child(i[0])[i[1:]]
  
  def __setitem__(self,i,x):
    self.check_i(i)
    if len(i)==0: raise ValueError('cant_set_toplevel')
    elif len(i)==1:
      if isinstance(i[0],tuple):
        attr,ilist = i[0]
        getattr(self,attr)[ilist] = x
      else: setattr(self,i[0],x)
    else: self.child(i[0])[i[1:]] = x

def sub_slots(x,match_fn,path=(),arr=None,match=False,recurse_into_matches=True):
  """given a BaseX in x, explore its ATTRS (doing the right thing for VARLEN).
  return a list of tree-paths (i.e. tuples) for tree children that match match_fn. The root elt won't match.
  """
  # todo: rename match to topmatch for clarity
  # todo: profiling suggests this getattr-heavy recursive process is the next bottleneck
  if arr is None: arr=[]
  if match and match_fn(x):
    arr.append(path)
    if not recurse_into_matches:
      return arr
  if isinstance(x, PathTree):
    for attr in x.ATTRS:
      val = getattr(x,attr)
      if attr in x.VARLEN:
        for i,elt in enumerate(val or ()):
          nextpath = path + ((attr,i),)
          sub_slots(elt,match_fn,nextpath,arr,True,recurse_into_matches)
      else:
        nextpath = path + (attr,)
        sub_slots(val,match_fn,nextpath,arr,True,recurse_into_matches)
  return arr

def flatten_tree(test, enumerator, exp):
  """test is function(exp) >> bool.
  mapper is function(expression) >> list of subexpressions.
  returns [subexpression, ...].
  """
  return sum((flatten_tree(test, enumerator, subx) for subx in enumerator(exp)), []) if test(exp) else [exp]
