"commands -- bodies for SQL commands"
# todo: separate the special versions of commands (i.e. insert_returning)

def emergency_cast(colx, value):
  """ugly: this is a huge hack. get serious about where this belongs in the architecture.
  For now, most types rely on being fed in as SubbedLiteral.
  """
  if colx.coltp.type.lower()=='boolean':
    if isinstance(value,sqparse2.NameX): value = value.name
    if isinstance(value,bool): return value
    return dict(true=True, false=False)[value.lower()] # keyerror if other
  else:
    return value # todo: type check?

def field_default(colx, table_name, tables_dict):
  "takes sqparse2.ColX, Table"
  raise NotImplementedError("this can't import sqex")
  if colx.coltp.type.lower() == 'serial':
    x = sqparse2.parse('select coalesce(max(%s),-1)+1 from %s' % (colx.name, table_name))
    return sqex.run_select(x, tables_dict, Table)[0]
  elif colx.not_null: raise NotImplementedError('todo: not_null error')
  else: return toliteral(colx.default)

def fix_rowtypes(self, row):
  if len(row)!=len(self.fields):
    raise ValueError('wrong # of values for table', self.name, self.fields, row)
  return map(toliteral, row)

def apply_defaults(self, row, tables_dict):
  "apply defaults to missing cols for a row that's being inserted"
  return [
    emergency_cast(colx, field_default(colx, self.name, tables_dict) if v is Missing else v)
    for colx,v in zip(self.fields,row)
  ]

def update(self, rowlist):
  "replace rows from rowlist using indexes"
  raise NotImplementedError('todo')
  raise NotImplementedError("delete old code below")
  nix = sqex.NameIndexer.ctor_name(self.name)
  nix.resolve_aonly(tables_dict,Table)
  if not all(isinstance(x,sqparse2.AssignX) for x in setx): raise TypeError('not_xassign',map(type,setx))
  match_rows=self.match(where,tables_dict,nix) if where else self.rows
  raise NotImplementedError('port old Evaluator')
  for row in match_rows:
    for x in setx: row[self.lookup(x.col).index]=sqex.Evaluator((row,),nix,tables_dict).eval(x.expr)
  if returning: return sqex.Evaluator((row,),nix,tables_dict).eval(returning)

def delete(self, rowlist):
  "use indexes from rowlist to delete"
  raise NotImplementedError('todo')
  raise NotImplementedError("delete old code below")
  # todo: what's the deal with nested selects in delete. does it get evaluated once to a scalar before running the delete?
  # todo: this will crash with empty where clause
  nix = sqex.NameIndexer.ctor_name(self.name)
  nix.resolve_aonly(tables_dict,Table)
  # todo(doc): why 'not' below?
  raise NotImplementedError('port old Evaluator')
  self.rows=[r for r in self.rows if not sqex.Evaluator((r,),nix,tables_dict).eval(where)]

def insert(self, fields, values, returning, tables_dict):
  print fields, values, returning
  raise NotImplementedError("this can't import sqex")
  nix = sqex.NameIndexer.ctor_name(self.name)
  nix.resolve_aonly(tables_dict,Table)
  expanded_row=self.fix_rowtypes(self.expand_row(fields,values) if fields else values)
  row=self.apply_defaults(expanded_row, tables_dict)
  # todo: check ColX.not_null here. figure out what to do about null pkey field
  for i,elt in enumerate(row):
    raise NotImplementedError('port old Evaluator')
    row[i]=sqex.Evaluator(row,nix,tables_dict).eval(elt)
  if self.pkey_get(row): raise pg.DupeInsert(row)
  self.rows.append(row)
  if returning: return sqex.Evaluator((row,),nix,tables_dict).eval(returning)
