"3-value logic (i.e. the way that boolean ops on nulls propagate up in the expression tree in SQL). doesn't rhyme with 'evil' but should."

class ThreeVL:
  "Implementation of sql's 3VL. Warning: use == != for comparing python values, not for 3vl comparison. Caveat emptor."
  # todo(awinter): is there any downside to using python True/False/None to make this work?
  def __init__(self, value):
    if value not in ('t', 'f', 'u'):
      raise ValueError(value)
    self.value = value

  def __repr__(self):
    return "<3vl %s>" % self.value

  def __eq__(self, other):
    if not isinstance(other, (bool, ThreeVL)):
      return False
    return self.value == other.value if isinstance(other, ThreeVL) else {True: 't', False: 'f'}[other] == self.value

  def __neq__(self, other):
    return not self == other

  def __bool__(self):
    # if self.value=='u': raise ValueError("can't cast 3VL 'unknown' to bool") # I think this is okay at top level
    return self.value == 't'

  @staticmethod
  def test(item):
    "this is the top-level output to SQL 'where' tests. At this level, 'u' *is* false"
    if not isinstance(item, (bool, ThreeVL)):
      raise TypeError(type(item)) # todo(awinter): test this on whereclause testing an int
    return item if isinstance(item, bool) else item.value == 't'
  # note below: the 3vl comparisons return a 3vl OR a bool

  @staticmethod
  def nein(item):
    "this is 'not' but not is a keyword so it's 'nein'"
    if not isinstance(item, (bool, ThreeVL)):
      raise TypeError(type(item))
    return not item if isinstance(item, bool) else ThreeVL(dict(t='f', f='t', u='u')[item.value])

  @staticmethod
  def andor(operator, left, right):
    # todo(awinter): does sql cast values to bools? e.g. nonempty strings, int 0 vs 1
    # is this the right one? https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_logic
    if operator not in ('and', 'or'):
      raise ValueError('unk_operator', operator)
    vals = left, right
    if not all(isinstance(item, (bool, ThreeVL)) for item in vals):
      raise TypeError(list(map(type, vals)))
    if ThreeVL('u') in vals:
      if operator == 'or' and True in vals:
        return True
      return False if False in vals else ThreeVL('u')
    left, right = list(map(bool, vals))
    return (left and right) if operator == 'and' else (left or right)

  @staticmethod
  def compare(operator, left, right):
    "this could be replaced by overloading but I want == to return a bool for 'in' use"
    # todo(awinter): what about nested 3vl like "(a=b)=(c=d)". is that allowed by sql? It will choke here if there's a null involved.
    if left is None or right is None:
      return ThreeVL('u')
    elif operator == '=':
      return left == right
    elif operator == '!=':
      return left != right
    elif operator == '>':
      return left > right
    elif operator == '<':
      return left < right
    else:
      raise ValueError('unk operator in compare', operator)
