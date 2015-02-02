import pytest

def test_3vl_basics():
  from pg13.threevl import ThreeVL
  assert bool(ThreeVL('t'))
  assert not bool(ThreeVL('f'))
  # with pytest.raises(ValueError): bool(ThreeVL('u')) # I think this needs to be false at the top level
  with pytest.raises(ValueError): ThreeVL('bad value')
  assert ThreeVL('f')!=ThreeVL('t')
  assert ThreeVL('t')==ThreeVL('t')
  assert [True,False,False]==[ThreeVL.test(ThreeVL(c)) for c in 'tfu']
  assert map(ThreeVL,'ftu')==[ThreeVL.nein(ThreeVL(c)) for c in 'tfu']
  assert map(ThreeVL,'ft')==map(ThreeVL.nein,(True,False))

def test_3vl_andor():
  from pg13.threevl import ThreeVL
  # test mixing bools and 3VLs
  assert ThreeVL.andor('and',True,ThreeVL('t'))
  assert not ThreeVL.andor('and',False,ThreeVL('t'))
  assert ThreeVL.andor('or',False,ThreeVL('t'))
  # now the truth table
  # https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_logic
  TABLE=[
    ('and','tt','t'),
    ('and','tu','u'),
    ('and','tf','f'),
    ('and','uu','u'),
    ('and','uf','f'),
    ('and','ff','f'),
    ('or','tt','t'),
    ('or','tu','t'),
    ('or','tf','t'),
    ('or','uu','u'),
    ('or','uf','f'),
    ('or','ff','f'),
  ]
  for op,(a,b),res in TABLE:
    assert ThreeVL(res)==ThreeVL.andor(op,ThreeVL(a),ThreeVL(b)), (op,(a,b),res)

@pytest.mark.xfail
def test_3vl_implication():
  raise NotImplementedError("figure out what SQL/postgres supports (and what implication means)")

def test_3vl_compare():
  from pg13.threevl import ThreeVL
  COMPS=[
    [False,('>',1,2)],
    [True,('>',1,0)],
    [True,('<',1,2)],
    [False,('<',1,0)],
    [False,('<',1,1)],
    [False,('!=',1,1)],
    [True,('!=',1,0)],
    [False,('=',1,0)],
    [True,('=',1,1)],
    [True,('!=',1,'a')],
    [False,('=',1,'a')],
    [ThreeVL('u'),('>',1,None)],
    [ThreeVL('u'),('!=',1,None)],
    [ThreeVL('u'),('!=',None,None)],
  ]
  for result,args in COMPS:
    assert result==ThreeVL.compare(*args),(result,args)
