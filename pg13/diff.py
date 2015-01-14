'diff.py -- diffing algorithms. see also js/diff.js for the browser-side equivalents of this' # todo: add JS to git

import re,sys,collections

Delta=collections.namedtuple('Delta','a b text') # todo: is text unicode even though PG is storing utf-8? find out and make a note.

def splitpreserve(s,redelim=r'\s'):
  'split, but preserves the delimiter so the string can be reassembled with double-spaces (etc) intact'
  pattern='[^%s]*%s*'%(redelim,redelim)
  return re.findall(pattern,s)

def applydiff(tokens,deltas):
  "tokens can be a string or a list of strings.\
  deltas is [Delta,...]. Delta.text is actually a tokenlist of the same type as tokens (string or list of string).\
  If tokens & tokenslist are strings, must be unicode for the offsets to match what JS produces.\
    (If they're lists, it doesn't matter; the offsets are relative to the lists, not internal to the strings)."
  sizechange=0
  for a,b,replace in deltas:
    tokens=tokens[:a+sizechange]+replace+tokens[b+sizechange:]
    sizechange+=len(replace)-(b-a)
  return tokens

def commonelts(a,b):
  "a and b are arrays. return set union."
  "this doesn't merit a function in python but I'm keeping it for congruence with JS"
  return set(a)&set(b)

def splitstatus(a,statusfn):
  'split sequence into subsequences based on binary condition statusfn. a is a list, returns list of lists'
  groups=[]; mode=None
  for elt,status in zip(a,map(statusfn,a)):
    assert isinstance(status,bool)
    if status!=mode: mode=status; group=[mode]; groups.append(group)
    group.append(elt)
  return groups

def groupelts(a,elts):
  'break list a into list of lists according to membership of elts (maintain order, break on membership boundaries)'
  'see tests for examples'
  return splitstatus(a,lambda x:x in elts)

def seqingroups(groups,seq):
  'helper for contigsub. takes the list of lists returned by groupelts and an array to check.\
  returns (groupindex,indexingroup,matchlen) of longest match or None if no match'
  if not (groups and seq): return None
  bestmatch=None,None,0
  if any(len(g)<2 for g in groups): raise ValueError('some subgroups have length < 2')
  for i,g in filter(lambda x:x[1][0],enumerate(groups)): # i.e. we're only interested in groups with common elements
    # begin starts at 0 so begin+1 starts at 1. (first elt of each group is the bool indicator)
    begin=0
    while 1:
      try: begin=g.index(seq[0],begin+1)
      except ValueError: break
      jmax=min(len(g)-begin,len(seq))
      for j in range(jmax):
        if g[begin+j]!=seq[j]: break
      else: j+=1 # so matchlen works below
      matchlen=min(j,jmax)
      if matchlen<bestmatch[2]: continue
      bestmatch=[i,begin,matchlen] # note: begin is an offset including the initial bool
  return bestmatch if bestmatch[2] else None

def ungroupslice(groups,gslice):
  'this is a helper for contigsub.'
  'coordinate transform: takes a match from seqingroups() and transforms to ungrouped coordinates'
  eltsbefore=0
  for i in range(gslice[0]): eltsbefore+=len(groups[i])-1
  x=eltsbefore+gslice[1]; return [x-1,x+gslice[2]-1]

def contigsub(a,b):
  'find longest common substring. return its slice coordinates (in a and b; see last line) or None if not found'
  'a and b are token lists'
  common=commonelts(a,b); groupsa=groupelts(a,common); groupsb=groupelts(b,common)
  bestmatch=[None,None,0]; bslice=None
  for i in range(len(groupsb)):
    if not groupsb[i][0]: continue
    if len(groupsb[i])-1<=bestmatch[2]: continue # this whole segment can't beat our best match so far
    for j in range(len(groupsb[i])):
      match=seqingroups(groupsa,groupsb[i][j:])
      if match and match[2]>bestmatch[2]: bestmatch=match; bslice=[i,j,match[2]]
      if match and match[2]>=(len(groupsb[i])/2.0): break # i.e. this is as good as we're going to get for groupsb[i], skip the rest. TODO: write a test for this
  return None if not bestmatch[2] else (ungroupslice(groupsa,bestmatch),ungroupslice(groupsb,bslice))

def subslice(inner,outer,section):
  'helper for rediff\
  outer is a slice (2-tuple, not an official python slice) in global coordinates\
  inner is a slice (2-tuple) on that slice\
  returns the result of sub-slicing outer by inner'
  if section=='head': return outer[0],outer[0]+inner[0]
  elif section=='tail': return outer[0]+inner[1],outer[1]
  elif section=='middle': return outer[0]+inner[0],outer[0]+inner[1]
  else: raise ValueError('section val %s not in head,middle,tail'%section)

def rediff(a,b,global_a_slice=None):
  "recursive diff (splits around longest substring and runs diff on head and tail remnants).\
  global_a_slice is used for recursion and should be left undefined in outer call.\
  returns a list of Delta tuples."
  if not (a or b): return []
  global_a_slice=global_a_slice or (0,len(a))
  csresult=contigsub(a,b)
  if not csresult: return (Delta(global_a_slice[0],global_a_slice[1],b),) # i.e. total replacement
  slicea,sliceb=csresult
  if slicea[0]==0 and sliceb[0]==0 and slicea[1]==len(a) and sliceb[1]==len(b): return [] # i.e. nochange
  head=rediff(a[:slicea[0]],b[:sliceb[0]],subslice(slicea,global_a_slice,'head'))
  tail=rediff(a[slicea[1]:],b[sliceb[1]:],subslice(slicea,global_a_slice,'tail'))
  return list(head)+list(tail)

def cumsum(a):
  cumulative=0; acum=a[:] # empty slice makes a copy
  for i,x in enumerate(a): cumulative+=x; acum[i]=cumulative
  return acum

def translate_diff(origtext,deltas):
  'take diff run on separated words and convert the deltas to character offsets'
  lens=[0]+cumsum(map(len,splitpreserve(origtext))) # [0] at the head for like 'length before'
  return [Delta(lens[a],lens[b],''.join(replace)) for a,b,replace in deltas]

def word_diff(a,b):
  'do diff on words but return character offsets'
  return translate_diff(a,rediff(splitpreserve(a),splitpreserve(b)))

def checkdiff(a,b,sp=True):
  'take diff of a to b, apply to a, return the applied diff so external code can check it against b'
  if sp: a=splitpreserve(a); b=splitpreserve(b)
  res=applydiff(a,rediff(a,b))
  if sp: res=''.join(res)
  return res
