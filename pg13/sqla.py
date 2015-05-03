"""sqla.py -- sqlalchemy dialect for pg13.
mostly copped from here: https://github.com/zzzeek/sqlalchemy_akiban/blob/master/sqlalchemy_akiban/dialect/base.py
sqlalchemy_akiban doesn't have a LICENSE file but setup.py has MIT license
"""

import sqlalchemy
from . import pgmock_dbapi2, __version__

class PG13Dialect(sqlalchemy.engine.default.DefaultDialect):
  name = 'pg13'
  supports_alter = False
  max_identifier_length = 1024 # probably longer
  supports_sane_rowcount = True
  supports_native_enum = False
  supports_native_boolean = True
  supports_sequences = False
  sequences_optional = True
  preexecute_autoincrement_sequences = False
  postfetch_lastrowid = True
  supports_default_values = True
  supports_empty_insert = False
  default_paramstyle = pgmock_dbapi2.paramstyle
  # ischema_names = ischema_names
  # colspecs = colspecs
  # statement_compiler = AkibanCompiler
  # ddl_compiler = AkibanDDLCompiler
  # type_compiler = AkibanTypeCompiler
  # preparer = AkibanIdentifierPreparer
  # execution_ctx_cls = AkibanExecutionContext
  # inspector = AkibanInspector
  # isolation_level = None
  # dbapi_type_map = {NESTED_CURSOR: NestedResult()}
  _backslash_escapes = True

  # @staticmethod
  # def dbapi(db=None, paramstyle=None):
  #   # todo: no idea what this does or why it's necessary. taking out the paramstyle arg seriously alters behavior.
  #   return pgmock_dbapi2.connect(db)
  
  def on_connect(self): pass
  def _get_default_schema_name(self, connection): return 'default' # hmm; I don't support schemas, do I?
  def has_schema(self, connection, schema): raise NotImplementedError("has_schema")
  def has_table(self, connection, table_name, schema=None):
    print table_name in connection.connection.connection.db
  def has_sequence(self, connection, sequence_name, schema=None): raise NotImplementedError("has sequence")
  def _get_server_version_info(self, connection): return __version__
  def get_schema_names(self, connection, **kw): raise NotImplementedError("schema names")
  def get_table_names(self, connection, schema=None, **kw):
    return connection.connection.connection.db.keys()
  def get_view_names(self, connection, schema=None, **kw): raise NotImplementedError("view names")
  def get_view_definition(self, connection, view_name, schema=None, **kw): raise NotImplementedError("view definition")
  def get_columns(self, connection, table_name, schema=None, **kw): return connection.pool.tables[table_name].fields
  def _get_column_info(self, name, format_type, default, notnull, schema): raise NotImplementedError
  def get_pk_constraint(self, connection, table_name, schema=None, **kw): return connection.pool.tables[table_name].pkey
  def get_foreign_keys(self, connection, table_name, schema=None, **kw): raise NotImplementedError
  def get_indexes(self, connection, table_name, schema, **kw): raise NotImplementedError

class PG13DBAPI2Dialect(PG13Dialect):
  use_native_unicode = True
  # execution_ctx_cls = AkibanPsycopg2ExecutionContext
  driver = 'dbapi2'
  supports_native_decimal = False
  @classmethod
  def dbapi(cls):
    return pgmock_dbapi2
  def on_connect(self): pass

  def create_connect_args(self, url):
    return (),{'db_id':int(url.host) if url.host else None}
