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
