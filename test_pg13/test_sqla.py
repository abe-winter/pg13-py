"test pg13 sqlalchemy dialect"

import sqlalchemy, pytest
import pg13.sqla

# travis should skip this because the sqlalchemy install is expensive
pytestmark = pytest.mark.travis_skip

sqlalchemy.dialects.registry.register("pg13.dbapi2", "pg13.sqla", "PG13Dialect")

def test_connect():
  sqlalchemy.create_engine("pg13+dbapi2://")
  raise NotImplementedError

def test_create_table():
  raise NotImplementedError

def test_insert():
  raise NotImplementedError

def test_select():
  raise NotImplementedError
