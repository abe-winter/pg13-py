"sqla.py -- sqlalchemy dialect for pg13"

import sqlalchemy
from . import pgmock_dbapi2

class PG13Dialect(sqlalchemy.engine.default.DefaultDialect):
  ""
  dbapi = pgmock_dbapi2.connect
  name = 'pg13'
  driver = 'dbapi2'
  positional = False
  paramstyle = pgmock_dbapi2.paramstyle
  convert_unicode = False
  encoding = 'utf-8'
  # statement_compiler
  # ddl_compiler
  # default_schema_name
  # execution_ctx_cls
  execute_sequence_format = tuple
  # preparer
  supports_alter = False
  max_identifier_length = 1024 # probably longer
  supports_unicode_statements = True
  # supports_unicode_binds
  supports_sane_rowcount = False # something about update and delete
  supports_sane_multi_rowcount = False # ?
  # preexecute_autoincrement_sequences
  implicit_returning = True
  dbapi_type_map = {}
  colspecs = {}
  supports_default_values = True
  supports_sequences = False
  sequences_optional = False
  supports_native_enum = False
  supports_native_boolean = True

  @staticmethod
  def connect(connection): return connection
  @staticmethod
  def dbapi(db_id=None): return pgmock_dbapi2.connect(db_id)
