"test_redismodel.py. some of these are marked integration and redis"

import pytest, redis
from pg13 import redismodel

TEST_DB = 6

class RM(redismodel.RedisModel):
  NAMESPACE = 'test_redismodel_RM'
  VERSION = 0
  KEY = [('a',int),('b',int)]
  VALUE = [('c',basestring),('d',list)]

def test_getitem():
  rm = RM([0,1], ['a',[]])
  assert rm['a'] == 0
  assert rm['b'] == 1
  assert rm['c'] == 'a'
  assert rm['d'] == []

@pytest.mark.integration
@pytest.mark.redis
def test_setget():
  con = redis.Redis(db=TEST_DB)
  rm = RM([0,1], ['a',[]])
  rm.save(con)
  rm2 = RM.get(con, 0, 1)
  assert rm == rm2
