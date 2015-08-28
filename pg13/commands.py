"commands -- bodies for SQL commands"
# todo: separate the special versions of commands (i.e. insert_returning)

from . import scope, planner, table2, sqex, sqparse2

class CommandError(StandardError): "base"
class NotNullError(CommandError): pass
class DuplicateInsert(CommandError): pass
class AggregationError(CommandError): pass

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

def field_default(colx, table_name, database):
  "takes sqparse2.ColX, Table"
  # todo: I think this can be an expression too but it HAS to be a scalar subquery
  # todo: DefaultX
  if colx.coltp.type.lower() == 'serial':
    raise NotImplementedError('run planner instead')
    x = sqparse2.parse('select coalesce(max(%s),-1)+1 from %s' % (colx.name, table_name))
    return sqex.run_select(x, database, table2.Table)[0]
  elif colx.not_null: raise NotNullError(table_name, colx)
  else: return table2.toliteral(colx.default)

def apply_defaults(database, table_, row):
  "apply defaults to missing cols for a row that's being inserted"
  return [
    emergency_cast(colx, field_default(colx, table_.alias, database) if v is table2.Missing else v)
    for colx, v in zip(table_.col_exprs, row)
  ]

def delete(self, rowlist):
  "use indexes from rowlist to delete"
  raise NotImplementedError('todo')
  raise NotImplementedError("delete old code below")
  # todo: what's the deal with nested selects in delete. does it get evaluated once to a scalar before running the delete?
  # todo: this will crash with empty where clause
  nix = sqex.NameIndexer.ctor_name(self.name)
  nix.resolve_aonly(tables_dict,Table)
  raise NotImplementedError('port old Evaluator')
  self.rows=[r for r in self.rows if not sqex.Evaluator((r,),nix,tables_dict).eval(where)]

def insert(database, expr):
  scope_ = scope.Scope.from_fromx(database, [expr.table])
  table_ = database[expr.table]
  vals = apply_defaults(database, table_, table_.fix_rowtypes(
    table_.expand_row(expr.cols, expr.values) if expr.cols else expr.values
  ))
  for i, elt in enumerate(vals):
    # warning: need dependency analysis on cols. 'insert into t (a,b) values (b,1)'
    vals[i] = sqex.Evaluator2(vals, scope_).eval(elt)
  if table_.pkey_get(vals):
    raise DuplicateInsert(expr, vals)
  table_.append(vals)
  if expr.ret is not None:
    if not isinstance(expr.ret, table2.Table):
      raise TypeError('expected Table in expr.ret vs', expr.ret)
    if not isinstance(expr.ret.expr, scope.SUBTABLE_TYPES):
      raise TypeError('expected subtable-expr table in expr.ret, not', expr.ret.expr)
    return sqex.Evaluator2(vals, scope_).eval(expr.ret, is_returning=True)

def query(database, fromx, wherex):
  "helper for commands that use a RowList"
  scope_ = scope.Scope.from_fromx(database, fromx)
  plan = planner.Plan.from_query(scope_, fromx, wherex)
  print 'query plan', plan
  return scope_, plan.run(scope_)

def do_assign(scope_, assigns, row):
  if not all(isinstance(x, sqparse2.AssignX) for x in assigns):
    # in theory the parser guarantees this; we're double-checking
    raise TypeError('not_all_AssignX', assigns, map(type, assigns))
  source_row = row.source.table.rows[row.source.index]
  for assign in assigns:
    lookup = row.source.table.lookup(assign.col)
    source_row[lookup.index] = sqex.Evaluator2(scope_, row).eval(assign.expr)

def update(database, expr):
  scope_, rowlist = query(database, expr.tables, expr.where)
  for row in rowlist:
    do_assign(scope_, expr.assigns, row)
  if expr.ret:
    # note: this is relying on row.vals being a reference
    return table.SelectResult([sqex.Evaluator2(row, scope_).eval(expr.ret) for row in rowlist])

def select(database, expr):
  scope_, rowlist = query(database, expr.tables, expr.where)
  if sqex.contains(expr.cols, sqex.consumes_rows):
    # case: aggregate query
    if expr.group or expr.order or expr.limit or expr.offset:
      raise NotImplementedError('todo: what to do with group/order/limit/offset in aggregate query?', {'group':expr.group, 'order':expr.order, 'limit':expr.limit, 'offset':expr.offset})
    if any(not sqex.contains(col, sqex.consumes_rows) for col in expr.cols.children):
      raise AggregationError("can't mix aggregate and non-aggregate fields") # note: sqlite allows this and returns something weird
    return table.SelectResult(sqex.Evaluator2(rowlist, scope_).eval(expr.cols), expr)
  else:
    if expr.order:
      rowlist.sort(key=lambda row:sqex.Evaluator2(row, scope_).eval(expr.order))
    if expr.group or expr.limit or expr.offset: raise NotImplementedError({'group':expr.group, 'order':expr.order, 'limit':expr.limit, 'offset':expr.offset})
    return table.SelectResult([sqex.Evaluator2(row, scope_).eval(expr.cols) for row in rowlist], expr)
