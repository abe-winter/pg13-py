import ast,os,setuptools
from pg13 import version

def get_version(fname):
  "grab __version__ variable from fname (assuming fname is a python file). parses without importing."
  assign_stmts = [s for s in ast.parse(open(fname).read()).body if isinstance(s,ast.Assign)]
  valid_targets = [s for s in assign_stmts if len(s.targets) == 1 and s.targets[0].id == '__version__']
  return valid_targets[-1].value.s # fail if valid_targets empty

setuptools.setup(
  name='pg13',
  version=version.__version__,
  description='sql evaluator for mocking',
  classifiers=[
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3 :: Only',
    'Topic :: Database',
    'Topic :: Software Development :: Interpreters',
    'Topic :: Software Development :: Testing',
  ],
  keywords=['sql','mocking','orm','database','testing'],
  author='Abe Winter',
  author_email='awinter.public+pg13@gmail.com',
  url='https://github.com/abe-winter/pg13-py',
  license='MIT',
  packages=setuptools.find_packages(),
  install_requires=['ply==3.11'],
  extras_require={
    'psyco':['psycopg2'],
    'sqla':['sqlalchemy'],
    'test':['pytest'],
  },
)
