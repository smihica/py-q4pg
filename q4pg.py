from contextlib import contextmanager
from datetime import datetime
import select, json, re, psycopg2
import psycopg2.extensions

def get_timespan(start):
    delta = (datetime.now() - start)
    return ((delta.days * 86400) + delta.seconds + (delta.microseconds / 1000000.0))

class QueueManager(object):

    LISTEN_TIMEOUT_INTERVAL_SECONDS = 1 # second

    def __init__(self,
                 dsn="", table_name="mq",
                 data_type="json",
                 data_length=1023,
                 excepted_times_to_ignore=0):
        self.parse_dsn(dsn)
        self.table_name   = table_name
        self.data_type    = data_type
        self.data_length  = data_length
        self.serializer   = lambda d: d
        self.deserializer = lambda d: d
        self.excepted_times_to_ignore = excepted_times_to_ignore
        if data_type is "json":
            self.serializer   = lambda d: json.dumps(d, separators=(',',':'))
            self.deserializer = lambda d: json.loads(d)
        self.setup_sqls()
        self.invoking_queue_id = None

    def parse_dsn(self, dsn):
        if dsn == "": # to use other session.
            self.dsn = None
        elif type(dsn) == str:
            mat = re.match(r'^(.+)://(.+?)(?::(.*)|)@(.+?)(?::(.*?)|)/(.+)', dsn)
            if mat: # is it url arg? (driver://username:password@hostname:port/dbname)
                driver, username, password, hostname, port, dbname = map(lambda i: mat.group(i), xrange(1,7))
                if not (driver in ('postgresql', 'postgres', 'psql', )):
                    raise Exception("Invalid driver (%s). QueueManager supports only 'postgresql://'." % driver)
                self.dsn = "user=%s host=%s dbname=%s" % (username, hostname, dbname, )
                self.dsn += (" port=%s" % port if port else "")
                self.dsn += (" password=%s" % password if password else "")
            else:
                self.dsn = dsn # psycopg2 arg.
        else:
            raise Exception("Invalid dsn argument given (%s)." % str(dsn))

    @contextmanager
    def session(self, other_sess):
        conn = None
        cur  = None
        if other_sess:
            cur  = other_sess
            yield (conn, cur)
        else:
            try:
                conn = psycopg2.connect(self.dsn)
                cur  = conn.cursor()
                yield (conn, cur)
            except:
                if conn and cur and (self.invoking_queue_id != None):
                    cur.execute(self.report_sql % (self.invoking_queue_id,))
                    res = cur.fetchone()
                    if res and res[0]:
                        conn.commit()
                raise
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
        return

    def setup_sqls(self):
        n = self.table_name
        self.create_table_sql = """
create table %s (
    id             serial          primary key,
    tag            varchar(31)     not null,
    content        varchar(%d),
    created_at     timestamp       not null default current_timestamp,
    except_times   integer         default 0,
    schedule       timestamp
);
create index %s_tag_idx         on %s(tag);
create index %s_created_at_idx  on %s(created_at);
create index %s_schedule        on %s(schedule);
""" % (n, self.data_length, n, n, n, n, n, n)
        self.drop_table_sql = """
drop table %s;
""" % (n,)
        self.insert_sql = """
insert into %s (tag, content, schedule) values ('%%s', '%%s', %%s);
""" % (n,)
        self.report_sql = """
update %s set except_times = except_times + 1
  where id = %%s and pg_try_advisory_lock(tableoid::int, id)
  returning pg_advisory_unlock(tableoid::int, id);
""" % (n,)
        self.select_sql = """
select * from %s
  where case
    when (tag = '%%s' and (schedule is null or schedule <= current_timestamp))
    then pg_try_advisory_lock(tableoid::int, id)
    else false
  end
  limit 1;
""" % (n,)
        self.list_sql = """
select * from %s
  where case when (tag = '%%s'%%s) then pg_try_advisory_lock(tableoid::int, id) else false end;
""" % (n,)
        self.count_sql = """
select count(*) from %s
  where case when (tag = '%%s'%%s) then pg_try_advisory_lock(tableoid::int, id) else false end;
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

    def create_table(self, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.create_table_sql)
            if conn: conn.commit()

    def drop_table(self, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.drop_table_sql)
            if conn: conn.commit()

    def reset_table(self, other_sess = None):
        self.drop_table(other_sess)
        self.create_table(other_sess)

    def sanitize(self, string):
        return string.replace("'", "''")

    def check_tag(self, tag):
        if "'" in tag:
            raise ValueError("Invalid tag-name. invalid char \"'\" is in tag-name.")
        return tag

    def enqueue(self, tag, data, other_sess = None, schedule = None):
        tag, data = (self.check_tag(tag), self.sanitize(self.serializer(data)), )
        with self.session(other_sess) as (conn, cur):
            cur.execute((self.insert_sql + (self.notify_sql % (tag,))) % (
                    tag, data,
                    schedule.strftime('timestamp \'%Y-%m-%d %H:%M:%S\'') if schedule else 'NULL'))
            if conn: conn.commit()

    @contextmanager
    def dequeue_item(self, tag, other_sess = None):
        tag = self.check_tag(tag)
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.select_sql % (tag,))
            res = cur.fetchone()
            if res:
                self.invoking_queue_id = res[0]
                if ((0 < self.excepted_times_to_ignore) and
                    (self.excepted_times_to_ignore <= int(res[4]))):
                    self.invoking_queue_id = None  # to ignore error reporting.
                    yield None
                else:
                    yield res
                cur.execute(self.ack_sql % (res[0],))
                if conn: conn.commit()
                self.invoking_queue_id = None
            else:
                yield res
            return

    @contextmanager
    def dequeue(self, tag, other_sess = None):
        with self.dequeue_item(tag, other_sess) as res:
            if res:
                yield self.deserializer(res[2])
            else:
                yield res
            return

    def listen_item(self, tag, timeout=None):
        tag         = self.check_tag(tag)
        wait_start  = datetime.now()
        interval    = self.LISTEN_TIMEOUT_INTERVAL_SECONDS
        while True:
            with self.session(None) as (conn, cur):
                cur.execute(self.select_sql % (tag,))
                res = cur.fetchone()
                if res:
                    self.invoking_queue_id = res[0]
                    if not ((0 < self.excepted_times_to_ignore) and
                            (self.excepted_times_to_ignore <= int(res[4]))):
                        yield res
                        wait_start = datetime.now()
                    cur.execute(self.ack_sql % (res[0],))
                    conn.commit()
                    self.invoking_queue_id = None
                    continue
                # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur.execute((self.listen_sql % (tag,)))
                if select.select([conn],[],[],interval) == ([],[],[]):
                    if timeout and (timeout <= get_timespan(wait_start)):
                        self.invoking_queue_id = None # to ignore error reporting.
                        yield None
                        wait_start = datetime.now()
                    continue
                conn.poll()
                if conn.notifies:
                    notify = conn.notifies.pop()
                    cur.execute(self.select_sql % (tag,))
                    res = cur.fetchone()
                    if res:
                        self.invoking_queue_id = res[0]
                        if not ((0 < self.excepted_times_to_ignore) and
                                (self.excepted_times_to_ignore <= int(res[4]))):
                            yield res
                            wait_start = datetime.now()
                        cur.execute(self.ack_sql % (res[0],))
                        conn.commit()
                        self.invoking_queue_id = None

    def listen(self, tag, timeout=None):
        for d in self.listen_item(tag, timeout=timeout):
            yield (self.deserializer(d[2]) if d != None else None)

    def dequeue_immediate(self, tag, other_sess = None):
        tag = self.check_tag(tag)
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.select_sql % (tag,))
            res = cur.fetchone()
            if res:
                cur.execute(self.ack_sql % (res[0],))
                if conn: conn.commit()
                return self.deserializer(res[2])
            return res

    def cancel(self, id, other_sess = None):
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.cancel_sql % (id,))
            res = cur.fetchone()
            if res and res[0]:
                if conn: conn.commit()
                return res[0]
            return res[0] if res else False

    def list(self, tag, other_sess = None, ignore_scheduled = True):
        tag = self.check_tag(tag)
        schedule = (" and (schedule is null or schedule <= current_timestamp)" if ignore_scheduled else "")
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.list_sql % (tag, schedule, ))
            res = cur.fetchall()
            return res

    def count(self, tag, other_sess = None, ignore_scheduled = True):
        tag = self.check_tag(tag)
        schedule = (" and (schedule is null or schedule <= current_timestamp)" if ignore_scheduled else "")
        with self.session(other_sess) as (conn, cur):
            cur.execute(self.count_sql % (tag, schedule, ))
            res = cur.fetchone()[0]
            return int(res)
