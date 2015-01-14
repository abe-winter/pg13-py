import re,pytest
# import pyximport; pyximport.install()
from pg13 import diff

def test_splitpreserve():
  strings=['a b c d e f g ','a b c d e   f    g','    a b c d e f g','    a b c d e f g    ','abcdefg','    ','']
  match=re.compile('^[^\s]*\s*$')
  for s in strings:
    split=diff.splitpreserve(s)
    assert s==''.join(split)
    assert all(match.match(tok) for tok in split)

def test_groupelts():
  a='socrates plato descartes napoleon democritus euclid voltaire robespierre'.split()
  greeks=set('socrates plato democritus euclid'.split())
  groups=diff.groupelts(a,greeks)
  assert groups==[
    [True,'socrates','plato'],
    [False,'descartes','napoleon'],
    [True,'democritus','euclid'],
    [False,'voltaire','robespierre']
  ]

def test_seqingroups_edgecases():
  # 1. empty groups
  with pytest.raises(ValueError): diff.seqingroups([[]],[1,2])
  with pytest.raises(ValueError): diff.seqingroups([[True]],[1,2])
  # 2. empty seq
  assert None is diff.seqingroups([[True,1,2]],[])
  # 3. all groups false
  assert None is diff.seqingroups([[False,1,2,3],[False,4,5,6]],[1,2,3])
  # 4. last element false
  assert diff.seqingroups([[True,1,2,4]],[1,2,3])==[0,1,2]
  assert diff.seqingroups([[True,1,2,3]],[1,2,3])==[0,1,3]

def test_seqingroups():
  ret=diff.seqingroups([[False,1,2,3],[True,1,2,3],[False,3,2]],[1,2,3])
  assert ret==[1,1,3]

def test_ungroupslice():
  groups=[[True,1,2,3,4],[True,4,5,6]]
  ret=diff.seqingroups(groups,[4,5,6])
  assert ret==[1,1,3]
  assert diff.ungroupslice(groups,ret)==[4,7]

def test_contigsub():
  a=diff.splitpreserve('two houses both alike in dignity eh?'); b=diff.splitpreserve('two house doth quoth alike in dignity ')
  lcs=diff.contigsub(a,b)
  match=a[lcs[0][0]:lcs[0][1]]
  assert match==['alike ','in ','dignity ']

def test_worddiff():
  a="testing testing testing testing testing testing."; b="testing testing  testing testing."
  deltas=diff.word_diff(a,b); print deltas; assert diff.applydiff(a,deltas)==b
  # check a multi-delta change:
  a='c d e f'; b='a b '+a+' g h'
  deltas=diff.word_diff(a,b)
  assert len(deltas)==2
  assert diff.applydiff(a,deltas)==b

@pytest.mark.xfail
def test_subslice():
  raise NotImplementedError,'todo next'

def test_rediff():
  a='two houses both alike in dignity eh?'; b='two houses doth quoth alike in dignity '
  assert diff.checkdiff(a,b)==b,'rediff test 1 failed'
  so_orig="NAME\nand even though the moment passed me by\nI still can't turn away\n(guitar)\n\nit's all the dreams you never thought you'd lose\ntossed along the way\n(guitar)\n\nletters that you never meant to send\nlost or blown away\n(guitar). (1 2 3 4)\n\nand now we're grown-up orphans tha never knew their names\nthat don't belong to no-one that's a shame\n\nyou could hide beside me maybe for a while\nand I won't tell no-one your name\nand I won't tell'm your name\n\n(guitar)\n\nscars are souvenirs your never lose\nthe past is never far"
  so_body="NAME\nand even though the moment passed me by\nI still can't turn away\n(guitar)\n\n\nit's all the dreams you never thought you'd lose\ntossed along the way\n(guitar)\n\nletters that you never meant to send\nlost or blown away\n\n(guitar). (1 2 3 4)\nand now we're grown-up orphans tha never knew their names\nthat don't belong to no-one that's a shame\n\nyou could hide beside me maybe for a while\nand I won't tell no-one your name\nand I won't tell'm your name\n\n(guitar)\n\nscars are souvenirs you never lose\nthe past is never far\n"
  assert diff.checkdiff(so_orig,so_body)==so_body,'rediff test 2 failed'

@pytest.mark.xfail
def test_translate_diff():
  raise NotImplementedError,'todo next'

@pytest.mark.xfail
def test_final_space_diffing():
  "known bug; remove xfail once this works and search around for final_space_diffing to make edits"
  assert diff.word_diff('abc','abc def')==[((3,3),' def')]
