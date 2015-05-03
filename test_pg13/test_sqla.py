"test pg13 sqlalchemy dialect"

import pytest
try:
  import sqlalchemy, sqlalchemy.ext.declarative, sqlalchemy.orm.session
  import pg13.sqla # does this matter for registry.register?
  from pg13 import pgmock_dbapi2
except ImportError: pass # survivable because sqla is legitimately missing on travis-ci or when -m "not travis_skip"

# travis should skip this because the sqlalchemy install is expensive
pytestmark = pytest.mark.travis_skip

@pytest.fixture(scope='session')
def sqla_dialect():
  sqlalchemy.dialects.registry.register("pg13", "pg13.sqla", "PG13DBAPI2Dialect")
  sqlalchemy.dialects.registry.register("pg13+dbapi2", "pg13.sqla", "PG13DBAPI2Dialect")

@pytest.fixture(scope='function')
def engine(sqla_dialect):
  db_id = pgmock_dbapi2.add_db() # todo: create database instead of this
  engine = sqlalchemy.create_engine("pg13://%i" % db_id)
  engine.pgmock_db_id = db_id
  return engine

@pytest.fixture(scope='function')
def Session(engine):
  return sqlalchemy.orm.session.sessionmaker(bind=engine)

@pytest.fixture(scope='function')
def Base():
  return sqlalchemy.ext.declarative.declarative_base()

@pytest.fixture(scope='function')
def TestTable(Base):
  class TestTable(Base):
    __tablename__ = 't1'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.Text)
  return TestTable

def test_connect(engine):
  engine.connect()

def test_create_table(engine, Base, TestTable):
  Base.metadata.create_all(engine)
  con = pgmock_dbapi2.connect(engine.pgmock_db_id)
  assert TestTable.__tablename__ in con.db

def test_insert(engine, Base, Session, TestTable):
  Base.metadata.create_all(engine)
  session = Session()
  session.add(TestTable(id=0,name='hello'))
  session.add(TestTable(id=1, name='goodbye'))
  session.commit()
  con = pgmock_dbapi2.connect(engine.pgmock_db_id)
  assert con.db['t1'].rows == [[0,'hello'],[1,'goodbye']]

def test_select(engine, Base, Session, TestTable):
  Base.metadata.create_all(engine)
  session = Session()
  session.add(TestTable(id=0,name='hello'))
  session.add(TestTable(id=1, name='goodbye'))
  assert ['hello','goodbye'] == [x.name for x in session.query(TestTable)]
  session.close() # this prevents 'rollback' printout in py.test output
