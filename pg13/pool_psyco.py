"""pool for psycopg2 backend.
this *doesn't* get imported by default because we don't want to have to install a zillion backends (most users only care about one).
"""

import psycopg2.pool,psycopg2
from . import pg,errors

class PgPoolPsyco(pg.PgPool):
  "see pg.PgPool class for tutorial"
  def __init__(self,dbargs):
    # http://stackoverflow.com/questions/12650048/how-can-i-pool-connections-using-psycopg-and-gevent
    self.pool = psycopg2.pool.ThreadedConnectionPool(5,10,dbargs) # I think that this is safe combined with psycogreen patching
  def select(self,qstring,vals=()):
    with self() as con,con.cursor() as cur:
      cur.execute(qstring,vals)
      for row in cur: yield row # yield stmt has to be in same function as with block to hijack it. todo(awinter): experiment and figure out what that meant.
  def commit(self,qstring,vals=()):
    with self() as con,con.cursor() as cur:
      return cur.execute(qstring,vals)
  def commitreturn(self,qstring,vals=()):
    "commit and return result. This is intended for sql UPDATE ... RETURNING"
    with self() as con,con.cursor() as cur:
      cur.execute(qstring,vals)
      return cur.fetchone()
  def close(self): self.pool.closeall()
  @contextlib.contextmanager
  def __call__(self):
    con = self.pool.getconn()
    try: yield con
    except psycopg2.IntegrityError as e: raise errors.PgPoolError(e)
    except: raise
    else: con.commit()
    finally: self.pool.putconn(con)
