"test pg13 sqlalchemy dialect"

import pytest
try:
  import sqlalchemy
  import pg13.sqla
except ImportError: pass # survivable because sqla is legitimately missing on travis-ci or when -m "not travis_skip"

# travis should skip this because the sqlalchemy install is expensive
pytestmark = pytest.mark.travis_skip

@pytest.fixture(scope='session')
def sqla_dialect():
  sqlalchemy.dialects.registry.register("pg13", "pg13.sqla", "PG13DBAPI2Dialect")
  sqlalchemy.dialects.registry.register("pg13+dbapi2", "pg13.sqla", "PG13DBAPI2Dialect")

def test_connect(sqla_dialect):
  engine = sqlalchemy.create_engine("pg13://3")
  con = engine.connect()
  raise NotImplementedError

def test_create_table(sqla_dialect):
  raise NotImplementedError

def test_insert(sqla_dialect):
  raise NotImplementedError

def test_select(sqla_dialect):
  raise NotImplementedError
