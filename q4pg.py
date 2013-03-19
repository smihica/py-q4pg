import psycopg2
import json
from contextlib import contextmanager

@contextmanager
def session(dsn):
    conn = None
    cur  = None
    try:
        conn = psycopg2.connect(dsn)
        cur  = conn.cursor()
        yield (conn, cur)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


create_table_sql = """
create table mq (
    id             serial          primary key,
    tag            varchar(31)     not null,
    content        varchar(1023),
    created_at     timestamp       not null default current_timestamp
);
create index mq_tag_idx         on mq(tag);
create index mq_created_at_idx  on mq(created_at);
"""

drop_table_sql = """
drop table mq;
"""

insert_sql = """
insert into mq (tag, content) values (%s, %s);
"""

select_sql = """
select * from mq
  where case when tag = %s then pg_try_advisory_lock(tableoid::int, id) else false end
  limit 1;
"""

list_sql = """
select * from mq
  where case when tag = %s then pg_try_advisory_lock(tableoid::int, id) else false end;
"""

count_sql = """
select count(*) from mq
  where case when tag = %s then pg_try_advisory_lock(tableoid::int, id) else false end;
"""

cancel_sql = """
delete from mq where id = %s and pg_try_advisory_lock(tableoid::int, id)
  returning pg_advisory_unlock(tableoid::int, id);
"""

ack_sql = """
delete from mq where id = %s
  returning pg_advisory_unlock(tableoid::int, id);
"""

class QueueManager(object):

    def __init__(self, dsn=""):
        self.dsn = dsn

    def create_table(self):
        with session(self.dsn) as (conn, cur):
            cur.execute(create_table_sql)
            conn.commit()

    def drop_table(self):
        with session(self.dsn) as (conn, cur):
            cur.execute(drop_table_sql)
            conn.commit()

    def reset_table(self):
        self.drop_table()
        self.create_table()

    def enqueue(self, tag, data):
        with session(self.dsn) as (conn, cur):
            cur.execute(insert_sql, (tag, json.dumps(data),))
            conn.commit()

    @contextmanager
    def dequeue(self, tag):
        with session(self.dsn) as (conn, cur):
            cur.execute(select_sql, (tag,))
            res = cur.fetchone()
            if res:
                yield json.loads(res[2])
                cur.execute(ack_sql, (res[0],))
                conn.commit()
            else:
                yield res
            raise StopIteration()

    def dequeue_immediate(self, tag):
        with session(self.dsn) as (conn, cur):
            cur.execute(select_sql, (tag,))
            res = cur.fetchone()
            if res:
                cur.execute(ack_sql, (res[0],))
                conn.commit()
                return json.loads(res[2])
            return res

    def cancel(self, id):
        with session(self.dsn) as (conn, cur):
            cur.execute(cancel_sql, (id,))
            res = cur.fetchone()
            if res and res[0]:
                conn.commit()
                return res[0]
            return res[0] if res else False

    def list(self, tag):
        with session(self.dsn) as (conn, cur):
            cur.execute(list_sql, (tag,))
            res = cur.fetchall()
            return res

    def count(self, tag):
        with session(self.dsn) as (conn, cur):
            cur.execute(count_sql, (tag,))
            res = cur.fetchone()[0]
            return int(res)

    def listen(self):
        pass

    def set_listener(self, tag, fn):
        pass
