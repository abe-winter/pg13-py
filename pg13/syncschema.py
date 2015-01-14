"""Notes on nested keys, i.e. children. A few questions:
1. should a check() always return unmentioned nested keys? >> no, make it a flag.
2. should nestkey resolution do more than one cycle? >> no, that's dangerous and slow.
3. should refkey resolution live in the model or the field? >> fields are what clients request, put er there.

I'm intentionally *not* using DB foreign key features to deal with refkeys because:
1. They create weird cascades and errors on delete
2. Will they work with the json structures I'm using? No idea.
3. They don't make 'select thing and refd children' any easier

Describe this as a 'distributed versioned foreign-key system'.
"""

import collections,binascii,ujson
from . import pg,diff,misc

COMPACTION_TIME_THRESH=600 # 10 minutes
COMPACTION_LEN_THRESH=256

class Syncable(object):
  "base class for isinstance/issubclass or whatever common features crop up.\
  version(), generate(), ser(), des()."
Increment=collections.namedtuple('Increment','utc value')
class SyncError(StandardError): "base class"
class BadBaseV(SyncError): ""
class ValidationFail(SyncError): ""

# these tuples follow the diff.Delta convention of not using good names for things. 'deltas' is just 'text' in VHString.
Change=collections.namedtuple('Change','version utc crc32 deltas')
Change2=collections.namedtuple('Change2','utc deltas crc32')

class RefKey(object):
  "stick these in the REFKEYS dict in a pg.Row descendant (i.e. a model)."
  def __init__(self,refmodel,pkey,getter=None):
    "refmodel is the class that's being looked up.\
    pkey is a list of fieldnames and None, has the same length as refmodel's pkey. Must have exactly 1 None, which gets interpolated."
    if pkey.count(None)!=1: raise ValueError('need exactly 1 null')
    self.refmodel,self.pkey,self.getter=refmodel,pkey,getter
  def pkeys(self,parent,field):
    "returns a list of pkey tuples by combining parent[field] with our attrs"
    template=[(parent[k] if k is not None else None) for k in self.pkey]
    inull=template.index(None)
    def mk(x):
      "helper for constructing pkey tuples in a list comp"
      template[inull]=x
      return tuple(template)
    val=parent[field]
    if self.getter is not None: return map(mk,self.getter(val))
    elif isinstance(val,VDList): return map(mk,val.generate())
    else: raise NotImplementedError(type(val))

def detect_change_mode(text,change):
  "returns 'add' 'delete' or 'internal'. see comments to update_changes for more details."
  # warning: some wacky diff logic (in python, probably in JS too) is making some adds / deletes look like replacements (see notes at bottom of diff.py)
  if len(change.deltas)>1: return 'internal'
  # todo below: why are blank deltas getting sent? is it a new seg thing? I'm picking 'add' because it has to be in some category and add is most likely next if this is a new seg.
  elif not change.deltas: return 'add'
  delta,=change.deltas # intentional crash if len(deltas)!=1
  if delta.a==delta.b and delta.a==len(text): return 'add'
  elif delta.b==len(text) and len(delta.text)==0: return 'delete'
  else: return 'internal'

def ucrc(ustring): return binascii.crc32(ustring.encode('utf-8'))
def mkchange(text0,text1,version,mtime):
  "return a Change diffing the two strings"
  return Change(version,mtime,ucrc(text1),diff.word_diff(text0,text1))

def update_changes(changes,newtext,change):
  "decide whether to compact the newest change into the old last; return new change list. assumes changes is safe to mutate.\
  note: newtext MUST be the result of applying change to changes, and is only passed to save doing the computation again."
  # the criteria for a new version are:
  # 1. mode change (modes are adding to end, deleting from end, internal edits)
  # 2. length changed by more than 256 chars (why power of 2? why not)
  # 3. time delta > COMPACTION_TIME_THRESH
  if not changes: return [change] # todo(awinter): needs test case
  if change.utc-changes[-1].utc>COMPACTION_TIME_THRESH:
    changes.append(change)
    return changes
  base=reduce(apply_change,changes[:-1],'')
  final=apply_change(base,changes[-1])
  prev_mode=detect_change_mode(base,changes[-1])
  cur_mode=detect_change_mode(final,change)
  if prev_mode==cur_mode and abs(len(newtext)-len(final)<COMPACTION_LEN_THRESH):
    changes[-1]=mkchange(base,newtext,change.version,change.utc)
  else: changes.append(change)
  return changes

def apply_change(text,change):
  "helper for VDiffString.text()"
  return diff.applydiff(text,change.deltas)

class SyncableString(Syncable):
  "base for VDString and VHString"
  def __init__(self,changes=None): self.changes=changes or []
  def ser(self,validate=True):
    if validate: self.validate(self.changes)
    return ujson.dumps(self.changes)
  @staticmethod
  def validate_base(changes):
    if not isinstance(changes,list): raise ValidationFail('non_list')
    if not all(isinstance(c,Change2) for c in changes): raise ValidationFail('non_changes')
  def version(self): return len(self.changes)
  def mtime(self): return self.changes[-1].utc if self.changes else None

class VDString(SyncableString):
  "strings assembled from chains of diffs"
  # todo: for a VDString that represents a JSON, don't commit unless it's valid. (ujson fast-path for validity?). may involve SpecialField
  def __init__(self,changes=None): super(VDString,self).__init__(changes)
  def apply(self,vbase,deltas,crc32,utc=None):
    # todo(DOC): here or in diff, explain where to use unicode vs utf-8
    # todo: think about compaction. it messes up my 'length of self.changes is the version number' approach.
    #   Can do it offline, after the fact, only for content older than a week, and it replaces deleted versions with nulls.
    #   Maybe also have a slow DB of full changes and a fast DB with compaction. Yuck. I'd be happier if this were a separate 'slow mo' column.
    if vbase!=len(self.changes): raise BadBaseV('got',vbase,'wanted',len(self.changes))
    newtext=diff.applydiff(self.generate(),deltas)
    if crc32!=ucrc(newtext): raise ValueError('crc_mismatch')
    return VDString(self.changes+[Change2(utc or misc.utcnow(),deltas,crc32)])
  def generate(self):
    "this *doesn't* check CRC on the assumption that apply() did"
    return reduce(apply_change,self.changes,'')
  @staticmethod
  def validate(changes):
    SyncableString.validate_base(changes)
    if not all(isinstance(c.deltas,list) and all(isinstance(d,diff.Delta) for d in c.deltas) for c in changes): raise ValidationFail('non_deltas')
    if changes and ucrc(VDString(changes).generate())!=changes[-1].crc32: raise ValidationFail('bad_hash')
  @classmethod
  def des(clas,underlying,validate=True):
    changes=[Change2(utc,[diff.Delta(*d) for d in deltas],crc) for utc,deltas,crc in ujson.loads(underlying)]
    if validate: clas.validate(changes)
    return clas(changes)
  @classmethod
  def create(clas,fromstring): return VDString().apply(0,[diff.Delta(0,0,fromstring)],ucrc(fromstring))

class VHString(SyncableString):
  "strings with past versions stored as whole strings, not diffs"
  def __init__(self,changes=None): super(VHString,self).__init__(changes)
  def apply(self,vbase,value,utc=None):
    if vbase!=len(self.changes): raise BadBaseV('got',vbase,'wanted',len(self.changes))
    return VHString(self.changes+[Change2(utc or misc.utcnow(),value,None)])
  def generate(self): return self.changes[-1].deltas if self.changes else '' # deltas is a string (not an array) here
  @staticmethod
  def validate(changes):
    SyncableString.validate_base(changes)
    if not all(isinstance(c.deltas,basestring) for c in changes): raise ValidationFail('nonstring_deltas')
  @classmethod
  def des(clas,underlying,validate=True):
    changes=[Change2(*tup) for tup in ujson.loads(underlying)]
    if validate: clas.validate(changes)
    return clas(changes)
  @classmethod
  def create(clas,fromstring): return VHString().apply(0,fromstring)

def apply_splice(a,splice):
  "mutate a *and* return it. a as list, splice as diff.Delta."
  a[splice.a:splice.b]=splice.text # text isn't always text. See diff comments.
  return a
class VDList(Syncable):
  "list of lists of deltas"
  def __init__(self,increments=None): self.increments=increments or []
  def apply(self,vbase,deltas,utc=None):
    "this doesn't modify the object -- it returns a new one (because of how pg.Row.update works)"
    if vbase!=len(self.increments): raise BadBaseV(vbase,len(self.increments))
    if not all(isinstance(d,diff.Delta) for d in deltas): raise TypeError('not_delta')
    utc=utc or misc.utcnow()
    # note: intentionally *not* checking that utc is monotonic because I don't want clock drift to ruin life
    reduce(apply_splice,deltas,self.generate()) # i.e. if this fails we have a problem
    return VDList(self.increments+[Increment(utc,deltas)])
  def generate(self):
    deltas=reduce(list.__add__,(i.value for i in self.increments),[])
    return reduce(apply_splice,deltas,[])
  @staticmethod
  def validate(incs):
    if not isinstance(incs,list): raise TypeError('non-list',type(incs))
    if not all(isinstance(x,Increment) and isinstance(x.value,list) and all(isinstance(d,diff.Delta) for d in x.value) for x in incs):
      raise TypeError('non-increment or non-delta')
  def ser(self,validate=True):
    if validate: self.validate(self.increments)
    return ujson.dumps(self.increments)
  @staticmethod
  def des(underlying,validate=True):
    incs=[Increment(utc,[diff.Delta(*so) for so in val]) for utc,val in ujson.loads(underlying)]
    if validate: VDList.validate(incs)
    return VDList(incs)
  @classmethod
  def create(clas,fromlist): return clas([Increment(misc.utcnow(),[diff.Delta(0,0,fromlist)])])
  def version(self): return len(self.increments)
  def mtime(self): return self.increments[-1].utc if self.increments else None
