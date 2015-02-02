"3-value logic (i.e. the way that boolean ops on nulls propagate up in the expression tree in SQL). doesn't rhyme with 'evil' but should."

class ThreeVL:
  "Implementation of sql's 3VL. Warning: use == != for comparing python values, not for 3vl comparison. Caveat emptor."
  # todo(awinter): is there any downside to using python True/False/None to make this work?
  def __init__(self,value):
    if value not in ('t','f','u'): raise ValueError(value)
    self.value=value
  def __repr__(self): return "<3vl %s>"%self.value
  def __eq__(self,other):
    if not isinstance(other,(bool,ThreeVL)): return False
    return self.value==other.value if isinstance(other,ThreeVL) else {True:'t',False:'f'}[other]==self.value
  def __neq__(self,other): return not (self==other)
  def __nonzero__(self):
    # if self.value=='u': raise ValueError("can't cast 3VL 'unknown' to bool") # I think this is okay at top level
    return self.value=='t'
  @staticmethod
  def test(x):
    "this is the top-level output to SQL 'where' tests. At this level, 'u' *is* false"
    if not isinstance(x,(bool,ThreeVL)): raise TypeError(type(x)) # todo(awinter): test this on whereclause testing an int
    return x if isinstance(x,bool) else x.value=='t'
  # note below: the 3vl comparisons return a 3vl OR a bool
  @staticmethod
  def nein(x):
    "this is 'not' but not is a keyword so it's 'nein'"
    if not isinstance(x,(bool,ThreeVL)): raise TypeError(type(x))
    return not x if isinstance(x,bool) else ThreeVL(dict(t='f',f='t',u='u')[x.value])
  @staticmethod
  def andor(operator,a,b):
    # todo(awinter): does sql cast values to bools? e.g. nonempty strings, int 0 vs 1
    # is this the right one? https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_logic
    if operator not in ('and','or'): raise ValueError('unk_operator',operator)
    vals=a,b
    if not all(isinstance(x,(bool,ThreeVL)) for x in vals): raise TypeError(map(type,vals))
    if ThreeVL('u') in vals:
      if operator=='or' and True in vals: return True
      return False if False in vals else ThreeVL('u')
    a,b=map(bool,vals)
    return (a and b) if operator=='and' else (a or b)
  @staticmethod
  def compare(operator,a,b):
    "this could be replaced by overloading but I want == to return a bool for 'in' use"
    # todo(awinter): what about nested 3vl like "(a=b)=(c=d)". is that allowed by sql? It will choke here if there's a null involved.
    f=({'=':lambda a,b:a==b,'!=':lambda a,b:a!=b,'>':lambda a,b:a>b,'<':lambda a,b:a<b}[operator])
    return ThreeVL('u') if None in (a,b) else f(a,b)
