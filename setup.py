import ast,os,setuptools

def get_version(fname):
  "grab __version__ variable from fname (assuming fname is a python file). parses without importing."
  assign_stmts = [s for s in ast.parse(open(fname).read()).body if isinstance(s,ast.Assign)]
  valid_targets = [s for s in assign_stmts if len(s.targets) == 1 and s.targets[0].id == '__version__']
  return valid_targets[-1].value.s # fail if valid_targets empty

setuptools.setup(
  name='pg13',
  version=get_version(os.path.join(os.path.dirname(__file__),'pg13/__init__.py')),
  description='sql models with built-in mocking framework',
  classifiers=[],
  keywords=['orm','postgres','mock','database','model'],
  author='Abe Winter',
  author_email='abe-winter@users.noreply.github.com',
  url='https://github.com/abe-winter/pg13-py',
  license='MIT',
  packages=setuptools.find_packages(exclude=('test*',)),
  install_requires=['pytest','psycopg2','ujson','msgpack-python','lrparsing'], # note: stubredis relies on redis, but doesn't get imported by __init__.
)
