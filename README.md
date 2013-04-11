### py-q4pg: A simple message queue using PostgreSQL in Python.

Lots of things are inspired from
 - http://d.hatena.ne.jp/n_shuyo/20090415/mq
 - https://github.com/fujiwara/perl-queue-q4pg-lite

__License: MIT__

### How to install py-q4pg

    $ python ./setup.py install

### Tutorial
```python
import q4pg

q = q4pg.QueueManager(
    dsn         = 'dbname=db1 user=user', # psycopg2's dsn argument. (default "")
    table_name  = 'mq',                   # name of the table to use. ("mq")
    data_type   = 'json',                 # stored data type : 'json' or 'text'. ("json")
    data_length = 1023)                   # data string max length. (1023)
```

##### To create queue table
```python
q.create_table()
```

##### enqueue
```python
q.enqueue('tag', {'the_data': 'must_be'})
q.enqueue('tag', {'json': 'serializable_data'})
q.enqueue('tag', {'more': 'data'})
```

##### dequeue
```python
with q.dequeue('tag') as dq:
    print dq
# => {'the_data': 'must_be'}

with q.dequeue('another-tag') as dq:
    print dq
# => None
```

##### dequeue-line
```python
with q.dequeue_item('tag') as dq:
    print dq
# => (1, 'tag', '{"the_data":"must_be"}', 0, datetime.datetime(...))

with q.dequeue('another-tag') as dq:
    print dq
# => None
```

##### show list
```python
q.list('tag')
# => [ (2, 'tag', '{"json":"serializable_data"}', 0, datetime.datetime(...)),
#      (3, 'tag', '{"more":"data"}', 0, datetime.datetime(...)), ]
```

##### dequeue (guard)
```python
# dequeue() is transactional.
# if you abort in the with statement,
# the queue is remained and can be gotten other runner or next time.

with q.dequeue('tag') as dq:
    print dq
    x = ( 1 / 0 )                     # <= Error

# => {'json': 'serializable_data'}
# => !!! Zero devision Error !!!

q.list('tag')
# => [ (3, 'tag', '{"more":"data"}', 0, datetime.datetime(...)),
#      (2, 'tag', '{"json":"serializable_data"}', 1, datetime.datetime(...)), ] <= remained and push tail.
#                                                ^^^ <= counted error time
```

##### dequeue (listen)
```python
for i in q.listen('tag'):             # waiting for queue notification.
    print i

# => ... waiting for a queue ...

>>> q.enqueue('tag', {'foo', 'bar'})  # someone push a queue.

# => {'foo', 'bar'}                   # get queue immediately
# => ... waiting for next queue ...

# listen() is also transactional.
# So if you abort in the for loop,
# the queue is remained and can be gotten other runner or next time.
```

##### dequeue-item (listen)
```python
for i in q.listen_item('tag'):        # waiting for queue notification.
    print i
# => (1, 'tag', {'foo', 'bar'}, 0, datetime.datetime(...))
```

##### dequeue (immediate)
```python
q.dequeue_immediate('tag')            # removed immediately, not transactional.
# => {'json': 'serializable_data'}
```

##### dequeue-item (immediate)
```python
q.dequeue_item_immediate('tag')       # removed immediately, not transactional.
# => (1, 'tag', {'json': 'serializable_data'}, 1, datetime.datetime(...))
```

##### counting items
```python
q.count('tag')
# => 1                                # the number of remainder queue.
```

##### cancel
```python
q.cancel(3)                           # specify id of queue.
# => True                             # success.
q.cancel(3)
# => False                            # failed to cancel or not found the queue.
```
