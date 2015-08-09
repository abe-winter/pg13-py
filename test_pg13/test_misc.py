import pytest
from pg13 import misc

@pytest.mark.xfail
def test_getterby(): raise NotImplementedError

def test_methonce():
  class C:
    @misc.meth_once
    def once(self): return 5
  c = C()
  assert c.once() == 5
  with pytest.raises(misc.CallOnceError): c.once()

def test_trace():
  def f2(): raise ValueError('whatever')
  def f1(): f2()
  trace = None
  try: f1()
  except: trace = misc.trace()
  assert trace is not None
  assert all(x.split(':')[0]=='test_misc.py' for x in trace)
  assert [x.split(':')[1] for x in trace]==['test_trace','f1','f2']

def test_multimap():
  # note: this is assuming stable sort (in assuming the values will be sorted); whatever, it's passing
  assert misc.multimap([(1,1),(1,2),(2,2),(3,3),(1,3)]) == {
    1: [1,2,3],
    2: [2],
    3: [3],
  }
