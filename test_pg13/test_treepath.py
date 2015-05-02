from pg13 import treepath

class PT(treepath.PathTree):
  def __init__(self, a, b): self.a, self.b = a, b

def test_get():
  pt = PT(1, [PT(2, 3), PT(4, 5)])
  assert pt[('b',0),'a'] == 2
  assert pt[('b',0),'b'] == 3
  assert pt[('b',1),'a'] == 4
  assert pt[('b',1),'b'] == 5
  assert pt['a',] == 1

def test_set():
  pt = PT(1, [PT(2, 3), PT(4, 5)])
  pt['a',] = 6
  pt[('b',1),] = 7
  pt[('b',0),'a'] = 8
  assert pt[('b',0),'a'] == 8
  assert pt[('b',1),] == 7
  assert pt[('a',)] == 6
