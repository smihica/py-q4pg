### py-q4pg: A simple message queue using PostgreSQL in Python.

Lots of things are inspired from
 - http://d.hatena.ne.jp/n_shuyo/20090415/mq
 - https://github.com/fujiwara/perl-queue-q4pg-lite

__License: MIT__

### How to install py-q4pg

    $ python ./setup.py install

### Tutorial

    >>> import q4pg
    >>>
    >>> q = q4pg.QueueManager('dbname=db1 user=user')

##### To create queue table

    >>> q.create_table(table_name='mq',     # name of the table to create.
                       data_type='json',    # 'json' or 'text'
                       data_length=1023)    # data string max length.

##### enqueue

    >>> q.enqueue('tag', {'the_data': 'must_be'})
    >>> q.enqueue('tag', {'json': 'serializable_data'})
    >>> q.enqueue('tag', {'more': 'data'})

##### dequeue

    >>> with q.dequeue('tag') as dq:
            print dq
        => {'the_data': 'is'}

    >>> with q.dequeue('another-tag') as dq:
            print dq
        => None

##### show list

    >>> q.list('tag')
        => [ (2, 'tag', '{"json":"serializable_data"}', datetime.datetime(...)),
             (3, 'tag', '{"more":"data"}', datetime.datetime(...)), ]

##### dequeue (guard)

    >>> with q.dequeue('tag') as dq:
           print dq
           x = ( 1 / 0 )                    # <= Error

        => {'json': 'serializable_data'}
        => !!! Zero devision Error !!!

    >>> q.list('tag')
        => [ (2, 'tag', '{"json":"serializable_data"}', datetime.datetime(...)),  # <= remained.
             (3, 'tag', '{"more":"data"}', datetime.datetime(...)), ]

##### dequeue (immediate)

    >>> q.dequeue_immediate('tag')          # commited immediately. cannot guard.
        => {'json': 'serializable_data'}

##### counting items

    >>> q.count('tag')
        => 1                                # the number of remainder queue.

##### cancel

    >>> q.cancel(3)                         # specify id of queue.
        => True                             # success.
    >>> q.cancel(3)
        => False                            # failed to cancel or not found the queue.
