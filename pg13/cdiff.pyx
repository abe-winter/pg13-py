import re,collections

Delta = collections.namedtuple('Delta', 'a b text')

cdef set commonelts(list a,list b):
  "a and b are arrays. return set union.\
  this doesn't merit a function in python but I'm keeping it for congruence with JS"
  return set(a)&set(b)

cdef list c_groupelts(list a,set elts):
  'break list a into list of lists according to membership of elts (maintain order, break on membership boundaries).\
  see tests for examples'
  cdef groups = []
  cdef bint initial = True
  cdef bint mode = False
  for elt in a:
    status = elt in elts
    if status != mode or initial:
      initial = False
      mode = status
      group = [mode]
      groups.append(group)
    group.append(elt)
  return groups

def groupelts(a,elts): return c_groupelts(a,elts)

def splitpreserve(s,redelim=r'\s'):
  'split, but preserves the delimiter so the string can be reassembled with double-spaces (etc) intact'
  pattern='[^%s]*%s*'%(redelim,redelim)
  return re.findall(pattern,s)

cdef struct BestMatch:
  bint matched
  bint error
  int groupindex
  int indexingroup
  int matchlen

cdef BestMatch c_seqingroups(list groups,list seq):
  'helper for contigsub. takes the list of lists returned by groupelts and an array to check.\
  returns (groupindex,indexingroup,matchlen) of longest match or None if no match'
  cdef BestMatch bestmatch
  cdef int begin = 0
  cdef int jamx
  bestmatch.matched = False
  bestmatch.error = False
  if not (groups and seq): return bestmatch
  bestmatch.groupindex,bestmatch.indexingroup,bestmatch.matchlen=-1,-1,0
  for g in groups:
    if len(g) < 2:
      bestmatch.error = True
      return bestmatch
  for i,g in enumerate(groups):
    if not g[0]: continue
    begin = 0
    while 1:
      try: begin = g.index(seq[0], begin + 1)
      except ValueError: break
      jmax = min(len(g) - begin, len(seq))
      for j in range(jmax):
        if g[begin + j] != seq[j]: break
      else: j += 1 # so matchlen works below
      matchlen = min(j, jmax)
      if matchlen < bestmatch.matchlen: continue
      bestmatch.matched = True
      bestmatch.groupindex = i
      bestmatch.indexingroup = begin # note: begin is an offset including the initial bool
      bestmatch.matchlen = matchlen
  return bestmatch

def seqingroups(groups,seq):
  cdef ret = c_seqingroups(groups, seq)
  if ret['error']: raise ValueError('some subgroups have len < 2')
  return [ret['groupindex'],ret['indexingroup'],ret['matchlen']] if ret['matched'] else None

cdef struct GlobalSlice:
  int a
  int b
  int len

cdef struct GSlice2:
  # not sure why GlobalSlice above has an extra field
  bint error
  int a
  int b

cdef struct ContigsubRet:
  bint matched
  bint error
  GSlice2 aslice
  GSlice2 bslice

cdef GSlice2 c_ungroupslice(list groups, GlobalSlice gslice):
  cdef int eltsbefore = 0
  for i in range(gslice.a): eltsbefore += len(groups[i]) - 1
  cdef int x = eltsbefore + gslice.b
  return GSlice2(x - 1, x + gslice.len - 1)

def ungroupslice(groups, gslice):
  'this is a helper for contigsub.\
  coordinate transform: takes a match from seqingroups() and transforms to ungrouped coordinates'
  return c_ungroupslice(groups, {'a':gslice[0], 'b':gslice[1], 'len':gslice[2]})

cdef ContigsubRet c_contigsub(list a,list b):
  cdef set common = commonelts(a,b)
  cdef list groupsa = c_groupelts(a, common)
  cdef list groupsb = c_groupelts(b, common)
  cdef BestMatch bestmatch = BestMatch(False, False, -1, -1, 0)
  cdef GlobalSlice bslice
  for i,gb in enumerate(groupsb):
    if not gb[0]: continue
    if len(gb)-1 <= bestmatch.matchlen: continue # this whole segment can't beat our best match so far
    for j in range(len(gb)):
      match = c_seqingroups(groupsa, gb[j:])
      if match.error: return ContigsubRet(False, True, [], [])
      if match.matched and match.matchlen > bestmatch.matchlen:
        bestmatch = match
        bslice = GlobalSlice(i, j, match.matchlen)
      if match.matched and match.matchlen >= len(gb)/2.: break # as good as it gets? todo doc: say why.
  return ContigsubRet(True, False,
    c_ungroupslice(groupsa, GlobalSlice(bestmatch.groupindex, bestmatch.indexingroup, bestmatch.matchlen)),
    c_ungroupslice(groupsb, bslice)
  ) if bestmatch.matchlen else ContigsubRet(False, False, [], [])

def contigsub(a,b):
  'find longest common substring. return its slice coordinates (in a and b; see last line) or None if not found\
  a and b are token lists'
  matched, error, aslice, bslice = c_contigsub(a, b)
  if error: raise ValueError
  return None if not matched else (aslice, bslice)

def checkdiff(a,b,sp=True):
  # todo: import from pure-py
  if sp: a=splitpreserve(a); b=splitpreserve(b)
  res=applydiff(a,rediff(a,b))
  if sp: res=''.join(res)
  return res

def applydiff(tokens,deltas):
  # todo: import from pure-py
  sizechange=0
  for a,b,replace in deltas:
    tokens=tokens[:a+sizechange]+replace+tokens[b+sizechange:]
    sizechange+=len(replace)-(b-a)
  return tokens

cdef GSlice2 c_subslice(GSlice2 inner, GSlice2 outer, str section):
  'helper for rediff\
  outer is a slice (2-tuple, not an official python slice) in global coordinates\
  inner is a slice (2-tuple) on that slice\
  returns the result of sub-slicing outer by inner'
  if section=='head': return GSlice2(False, outer.a, outer.a+inner.a)
  elif section=='tail': return GSlice2(False, outer.a+inner.b, outer.b)
  elif section=='middle': return GSlice2(False, outer.a+inner.a,outer.a+inner.b)
  else: return GSlice2(True, -1, -1)

cdef list c_rediff(list a, list b, GSlice2 global_a_slice):
  "recursive diff (splits around longest substring and runs diff on head and tail remnants).\
  global_a_slice is used for recursion and should be left undefined in outer call.\
  returns a list of Delta tuples."
  if not (a or b): return []
  csret = c_contigsub(a, b)
  if not csret.matched: return [Delta(global_a_slice.a, global_a_slice.b, b)] # i.e. total replacement
  if csret.aslice.a==0 and csret.bslice.a==0 and csret.aslice.b==len(a) and csret.bslice.b==len(b):
    return [] # i.e. nochange
  head = c_rediff(a[:csret.aslice.a], b[:csret.bslice.a], c_subslice(csret.aslice, global_a_slice, 'head'))
  tail = c_rediff(a[csret.aslice.a:], b[csret.bslice.b:], c_subslice(csret.aslice, global_a_slice, 'tail'))
  return head + tail
  """
  if not (a or b): return []
  global_a_slice=global_a_slice or (0,len(a))
  csresult=contigsub(a,b)
  if not csresult: return (Delta(global_a_slice[0],global_a_slice[1],b),) # i.e. total replacement
  slicea=csresult[0]; sliceb=csresult[1]
  if slicea[0]==0 and sliceb[0]==0 and slicea[1]==len(a) and sliceb[1]==len(b): return [] # i.e. nochange
  head=rediff(a[:slicea[0]],b[:sliceb[0]],subslice(slicea,global_a_slice,'head'))
  tail=rediff(a[slicea[1]:],b[sliceb[1]:],subslice(slicea,global_a_slice,'tail'))
  return list(head)+list(tail)
  """

cdef list cumsum(list a):
  cdef int cumulative = 0
  cdef list acum = [0] * len(a)
  for i,x in enumerate(a):
    cumulative += x
    acum[i] = cumulative
  return acum

def translate_diff(origtext,deltas):
  'take diff run on separated words and convert the deltas to character offsets'
  lens=[0]+cumsum(map(len,splitpreserve(origtext))) # [0] at the head for like 'length before'
  return [Delta(lens[a],lens[b],''.join(replace)) for a,b,replace in deltas]

def rediff(a, b): return c_rediff(a, b, GSlice2(False, 0, len(a)))

def word_diff(a,b):
  'do diff on words but return character offsets'
  return translate_diff(a, rediff(splitpreserve(a), splitpreserve(b)))
