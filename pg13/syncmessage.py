"""messaging for syncschema.
this sits on top of pg.Row-based schemas and negotiates client sync
Lightweight logic means app has to do some lifting to integrate this into its dataflow.
Here's a typical syncschema transaction:
 ____________________________________________________
| CLIENT     | APP LAYER             | SS LIB        |
+------------+-----------------------+---------------+
| send msg ->> pass                ->>               |
|            |                       <<- translate   |
|            | load models (db read) |               |
|            | load refkey models  ->>               |
|            |                       | update models |
|            |                       <<- mk reply    |
|            | post-update triggers  |               | # like update full-text index
|            <<- send reply          |               |
'------------+-----------------------+---------------'
"""

import ujson,collections,logging
from . import syncschema,pg,diff,misc

# todo delete: vnew isn't used, get rid of the field
SerialDiff=collections.namedtuple('SerialDiff','vbase vnew mtime deltas crc')
CheckStale=collections.namedtuple('CheckStale','sent_version')
FieldKey=collections.namedtuple('FieldKey','nombre pkey field')
ModelInfo=collections.namedtuple('ModelInfo','name row pkey')

def translate_key(jsonkey):
  "helper for translate_*"
  nombre,pkey,field=ujson.loads(jsonkey)
  return FieldKey(nombre,tuple(pkey),field)
def parse_serialdiff(sd_dict):
  "helper for translate_check"
  if isinstance(sd_dict,list):
    if len(sd_dict)!=2 or sd_dict[0]!='checkstale': raise NotImplementedError(sd_dict[0],len(sd_dict))
    return CheckStale(sd_dict[1])
  if isinstance(sd_dict['deltas'],list): # i.e. for VHString the whole deltas field is a single string
    # warning below: Delta.text will be a list sometimes. always?
    sd_dict['deltas']=[diff.Delta(d['slice']['a'],d['slice']['b'],d['replace']) for d in sd_dict['deltas']]
  return SerialDiff(**sd_dict)
def translate_update(blob):
  "converts JSON parse output to self-aware objects"
  # note below: v will be int or null
  return {translate_key(k):parse_serialdiff(v) for k,v in blob.items()}
def translate_check(blob):
  "JSON blob to objects"
  return {translate_key(k):v for k,v in blob.items()}

def fkapply(models,pool,fn,missing_fn,(nombre,pkey,field),*args):
  "wrapper for do_* funcs to call process_* with missing handler. Unpacks the FieldKey."
  if (nombre,pkey) in models: return fn(pool,models[nombre,pkey],field,*args)
  else: return missing_fn(pool,field,*args) if missing_fn else ['missing']

def process_update(pool,model,field,sdiff):
  try: syncable=model[field]
  except pg.FieldError: return ['?field']
  if isinstance(sdiff,CheckStale): return ['chkstale',syncable.version(),syncable.generate()]
  elif not isinstance(sdiff,SerialDiff): raise TypeError(type(sdiff))
  if syncable.version()>sdiff.vbase:
    # todo: if syncable.version()==sdiff.vnew and 'todo: test that the data is the same': return ['same',syncable.version()]
    return ['merge!',syncable.version(),syncable.generate()]
  elif syncable.version()<sdiff.vbase: return ['nobase!',syncable.version()] # this means a logic error or data loss
  else:
    try:
      if syncable.version()!=sdiff.vbase: raise ValueError(syncable.version(),sdiff.vbase) # probably a type error
      # todo: think about whether making this an array type in PG reduces IO by a serious amount
      # todo: all the apply meths should be taking a SerialDiff
      if isinstance(syncable,syncschema.VDList): applied=syncable.apply(sdiff.vbase,sdiff.deltas)
      elif isinstance(syncable,syncschema.VDString): applied=syncable.apply(sdiff.vbase,sdiff.deltas,sdiff.crc)
      elif isinstance(syncable,syncschema.VHString): applied=syncable.apply(sdiff.vbase,sdiff.deltas)
      else: raise TypeError('unsupported syncable',type(syncable))
      model.update(pool,{field:applied})
      syncable=model[field] # because update() replaced it
      return ['ok',syncable.version()]
    except Exception as e:
      logging.error('in syncmessage: %s %s %s'%(misc.trace()[-8:],e.__class__.__name__,e))
      return ['error!'] # todo: log it or report it. needs to also show up in sentry or whatever I pick
def missing_update(pool,field,sdiff):
  return ['chkstale',None,None] if isinstance(sdiff,CheckStale) else ['missing']
def do_update(pool,request,models):
  "unlike *_check() below, update doesn't worry about missing children"
  return {k:fkapply(models,pool,process_update,missing_update,k,v) for k,v in request.items()}

def process_check(pool,model,field,version):
  "helper for do_check. version is an integer or null. returns ..."
  try: syncable=model[field]
  except pg.FieldError: return ['?field']
  if syncable.version()>version: # this includes version=None. this is the load case as well as update.
    return ['here',syncable.version(),syncable.generate()] # 'here' as in 'here, take this' or 'here you go'
  elif syncable.version()==version: return ['ok',version]
  elif syncable.version()<version: return ['upload',syncable.version()]
  else: raise RuntimeError("shouldn't get here")
def add_missing_children(models,request,include_children_for,modelgb):
  "helper for do_check. mutates request"
  for (nombre,pkey),model in models.items():
    for modelclass,pkeys in model.refkeys(include_children_for.get(nombre,())).items():
      # warning: this is defaulting to all fields of child object. don't give clients a way to restrict that until there's a reason to.
      childname=modelgb['row',modelclass].name
      for childfield,cftype in modelclass.FIELDS:
        # warning: issubclass fails in SpecialField tuple case. (a) issubclass sucks and (b) be more static about types.
        if not isinstance(cftype,pg.SpecialField) or not issubclass(cftype.pytype,syncschema.Syncable): continue
        request.update({rk:None for rk in [FieldKey(childname,pkey,childfield) for pkey in pkeys] if rk not in request})
  return request # the in-place updated original
def do_check(pool,request,models,include_children_for,modelgb):
  "request is the output of translate_check. models a dict of {(model_name,pkey_tuple):model}.\
  ICF is a {model_name:fields_list} for which we want to add nulls in request for missing children. see AMC for how it's used.\
  The caller should have gone through the same ICF logic when looking up models so the arg has all the refs the DB knows.\
  modelgb is misc.GetterBy<ModelInfo>, used by AMC for resolution."
  add_missing_children(models,request,include_children_for,modelgb)
  return {k:fkapply(models,pool,process_check,None,k,v) for k,v in request.items()}
