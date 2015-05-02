"test_redismodel.py. some of these are marked integration and redis"

import pytest
try: 
  import redis
  from pg13 import redismodel
except ImportError: pass # this needs to be survivable on travis-ci because we don't install redis

# travis should skip this because the redis install is expensive
pytestmark = pytest.mark.travis_skip

# ugly: pie-in-the-sky guess to prevent interfering with prod redis data on my laptop
TEST_DB = 6

@pytest.fixture
def RM():
  class RM(redismodel.RedisModel):
    NAMESPACE = 'test_redismodel_RM'
    VERSION = 0
    KEY = [('a',int),('b',int)]
    VALUE = [('c',basestring),('d',list)]
  return RM

def test_getitem(RM):
  rm = RM([0,1], ['a',[]])
  assert rm['a'] == 0
  assert rm['b'] == 1
  assert rm['c'] == 'a'
  assert rm['d'] == []

@pytest.mark.integration
@pytest.mark.redis
def test_setget(RM):
  con = redis.Redis(db=TEST_DB)
  rm = RM([0,1], ['a',[]])
  rm.save(con)
  rm2 = RM.get(con, 0, 1)
  assert rm == rm2
