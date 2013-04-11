from contextlib import contextmanager
import select
import json
import psycopg2
import psycopg2.extensions

LISTEN_TIMEOUT_SECONDS = 60 # 1min

class QueueManager(object):

    def __init__(self,
                 dsn="", table_name="mq",
                 data_type="json", data_length=1023):
        self.dsn          = dsn
        self.table_name   = table_name
        self.data_type    = data_type
        self.data_length  = data_length
        self.serializer   = lambda d: d
        self.deserializer = lambda d: d
        if data_type is "json":
            self.serializer   = lambda d: json.dumps(d, separators=(',',':'))
            self.deserializer = lambda d: json.loads(d)
        self.setup_sqls()
        self.invoking_queue_id = None

    @contextmanager
    def session(self):
        conn = None
        cur  = None
        try:
            conn = psycopg2.connect(self.dsn)
            cur  = conn.cursor()
            yield (conn, cur)
        except:
            if conn and cur and (self.invoking_queue_id != None):
                cur.execute(self.report_sql, (self.invoking_queue_id,))
                res = cur.fetchone()
                if res and res[0]:
                    conn.commit()
            raise
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def setup_sqls(self):
        n = self.table_name
        self.create_table_sql = """
create table %s (
    id             serial          primary key,
    tag            varchar(31)     not null,
    content        varchar(%d),
    created_at     timestamp       not null default current_timestamp
    except_times   integer         default 0,
);
create index %s_tag_idx         on %s(tag);
create index %s_created_at_idx  on %s(created_at);
""" % (n, self.data_length, n, n, n, n,)
        self.drop_table_sql = """
drop table %s;
""" % (n,)
        self.insert_sql = """
insert into %s (tag, content) values (%%s, %%s);
""" % (n,)
        self.report_sql = """
update %s set except_times = except_times + 1
  where id = %%s and pg_try_advisory_lock(tableoid::int, id)
  returning pg_advisory_unlock(tableoid::int, id);
""" % (n,)
        self.select_sql = """
select * from %s
  where case when tag = %%s then pg_try_advisory_lock(tableoid::int, id) else false end
  limit 1;
""" % (n,)
        self.list_sql = """
select * from %s
  where case when tag = %%s then pg_try_advisory_lock(tableoid::int, id) else false end;
""" % (n,)
        self.count_sql = """
select count(*) from %s
  where case when tag = %%s then pg_try_advisory_lock(tableoid::int, id) else false end;
""" % (n,)
        self.cancel_sql = """
delete from %s where id = %%s and pg_try_advisory_lock(tableoid::int, id)
  returning pg_advisory_unlock(tableoid::int, id);
""" % (n,)
        self.ack_sql = """
delete from %s where id = %%s
  returning pg_advisory_unlock(tableoid::int, id);
""" % (n,)
        self.notify_sql = """
notify %s;
"""
        self.listen_sql = """
listen %s;
"""

    def create_table(self):
        with self.session() as (conn, cur):
            cur.execute(self.create_table_sql)
            conn.commit()

    def drop_table(self):
        with self.session() as (conn, cur):
            cur.execute(self.drop_table_sql)
            conn.commit()

    def reset_table(self):
        self.drop_table()
        self.create_table()

    def enqueue(self, tag, data):
        with self.session() as (conn, cur):
            cur.execute((self.insert_sql + (self.notify_sql % (tag,))),
                        (tag, self.serializer(data),))
            conn.commit()

    @contextmanager
    def dequeue_item(self, tag):
        with self.session() as (conn, cur):
            cur.execute(self.select_sql, (tag,))
            res = cur.fetchone()
            if res:
                self.invoking_queue_id = res[0]
                yield res
                cur.execute(self.ack_sql, (res[0],))
                conn.commit()
                self.invoking_queue_id = None
            else:
                yield res
            return

    @contextmanager
    def dequeue(self, tag):
        with self.dequeue_item(tag) as res:
            if res:
                yield self.deserializer(res[2])
            else:
                yield res
            return

    def listen_item(self, tag):
        while True:
            with self.session() as (conn, cur):
                cur.execute(self.select_sql, (tag,))
                res = cur.fetchone()
                if res:
                    self.invoking_queue_id = res[0]
                    yield res
                    cur.execute(self.ack_sql, (res[0],))
                    conn.commit()
                    self.invoking_queue_id = None
                    continue
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur.execute((self.listen_sql % (tag,)))
                if select.select([conn],[],[],LISTEN_TIMEOUT_SECONDS) == ([],[],[]):
                    continue
                conn.poll()
                if conn.notifies:
                    notify = conn.notifies.pop()
                    cur.execute(self.select_sql, (tag,))
                    res = cur.fetchone()
                    if res:
                        self.invoking_queue_id = res[0]
                        yield res
                        cur.execute(self.ack_sql, (res[0],))
                        conn.commit()
                        self.invoking_queue_id = None

    def listen(self, tag):
        for d in self.listen_item(tag):
            yield self.deserializer(d[2])

    def dequeue_immediate(self, tag):
        with self.session() as (conn, cur):
            cur.execute(self.select_sql, (tag,))
            res = cur.fetchone()
            if res:
                cur.execute(self.ack_sql, (res[0],))
                conn.commit()
                return self.deserializer(res[2])
            return res

    def cancel(self, id):
        with self.session() as (conn, cur):
            cur.execute(self.cancel_sql, (id,))
            res = cur.fetchone()
            if res and res[0]:
                conn.commit()
                return res[0]
            return res[0] if res else False

    def list(self, tag):
        with self.session() as (conn, cur):
            cur.execute(self.list_sql, (tag,))
            res = cur.fetchall()
            return res

    def count(self, tag):
        with self.session() as (conn, cur):
            cur.execute(self.count_sql, (tag,))
            res = cur.fetchone()[0]
            return int(res)
