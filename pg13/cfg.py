"cfg.py -- control-flow graph resolver & collection class for SQL expressions"

from . import treepath

def command_graph(top_expression):
  "takes a sqparse2.BaseX. Returns a tree (what class?) of Table."
  raise NotImplementedError

class SerializedCommand:
  """represents a command as a sequence of expression subtrees rather than a single tree.
  The tree is broken into subtrees (blocks) based on dependencies, with upstream calculations
    appering earlier in the subtree list.
  """
  def __init__(self, subtrees):
    self.subtrees = subtrees

def expression_graph(command_graph):
  """Takes the output of command_graph().
  Returns an analogous tree with Tables edited so their .expr attr is a SerializedCommand.
  """
  raise NotImplementedError
