#!/usr/bin/env python
import sys, q4pg, datetime

q = None

def gettable():
    return 'test_table_%s' % str(datetime.datetime.now().microsecond).replace(' ', '')

def create_table():
    q.create_table()

def drop_table():
    q.drop_table()

def enqueue():
    q.enqueue('tag', {'the_data': 'must_be'})
    q.enqueue('tag', {'json': 'serializable_data'})
    q.enqueue('tag', {'more': 'data'})
    q.enqueue('tag', {'more': 'data2'})
    q.enqueue('tag', {'more': 'data3'})
    if len(q.list('tag')) != 5:
        raise Exception("failed enqueue 1")
    else:
        print 'OK enqueue 1'

def dequeue():
    with q.dequeue('tag') as dq:
        if dq != {'the_data': 'must_be'}:
            raise Exception("failed dequeue 1")
        else:
            print 'OK dequeue 1'
    with q.dequeue('another_tag') as dq:
        if dq != None:
            raise Exception("failed dequeue 2")
        else:
            print 'OK dequeue 2'

def dequeue_line():
    with q.dequeue_item('tag') as dq:
        if (not (dq[1] == 'tag' and dq[2] == u'{"json":"serializable_data"}')):
            raise Exception("failed dequeue line 1")
        else:
            print 'OK dequeue line 1'
    with q.dequeue_item('another_tag') as dq:
        if dq != None:
            raise Exception("failed dequeue 2")
        else:
            print 'OK dequeue line 2'

def show_list():
    lis = q.list('tag')
    if len(lis) != 3:
        raise Exception("failed list 1")
    else:
        print 'OK list 1'

def dequeue_transaction():
    before = q.list('tag')[0]
    try:
        with q.dequeue('tag') as dq:
            x = ( 1 / 0 )                     # <= Error
    except:
        pass
    after = q.list('tag')[-1] # moved last
    if not (before[0] == after[0] and     # check if id is same
            (before[4] + 1) == after[4]): # check if error +1?
        raise Exception("failed dequeue_transaction 1")
    else:
        print 'OK dequeue_transaction 1'

def dequeue_listen():
    qs = []
    for i in q.listen('tag'):
        qs.append(i)
        if i == {'more': 'data'}:
            break
    if qs != [{'more': 'data2'},
              {'more': 'data3'},
              {'more': 'data'}]:
        raise Exception("failed dequeue_listen 1")
    else:
        print 'OK dequeue_listen 1'

def dequeue_item_listen():
    q.enqueue('tag', {'new': 'data'})
    qs = None
    for i in q.listen_item('tag'):
        qs = i
        if i[0] == 6:
            break
    if qs[0] != 6 or qs[1] != 'tag' or qs[2] != '{"new":"data"}':
        raise Exception("failed dequeue_item_listen 1")
    else:
        print 'OK dequeue_item_listen 1'

def count_item():
    if q.count('tag') != 1:
        raise Exception("failed count_item 1")
    else:
        print 'OK count_item 1'
    q.enqueue('tag', {'new': 'data2'})
    if q.count('tag') != 2:
        raise Exception("failed count_item 2")
    else:
        print 'OK count_item 2'

def cancel():
    if ((not q.cancel(6)) or
        (not q.cancel(7)) or
        q.cancel(3) or
        q.count('tag') != 0):
        raise Exception("failed cancel 1")
    else:
        print 'OK cancel 1'

def excepted_times_to_ignore():
    q.excepted_times_to_ignore = 2
    q.enqueue('tag', {'err': 'data1'})
    if (q.count('tag') == 1):
        try:
            with q.dequeue('tag') as dq:
                x = ( 1 / 0 )              # <= Error 1
        except:
            pass
        if (q.list('tag')[0][4] == 1):
            try:
                with q.dequeue('tag') as dq:
                    x = ( 1 / 0 )          # <= Error 2
            except:
                pass
            if (q.list('tag')[0][4] == 2):
                try:
                    with q.dequeue('tag') as dq:
                        if (dq == None):
                            print 'OK excepted_times_to_ignore 1'
                            return
                except:
                    pass
    raise Exception("failed excepted_times_to_ignore 1")


def excepted_times_to_ignore_listen():
    q.excepted_times_to_ignore = 2
    q.enqueue('tag', {'err': 'data1'})
    err = False
    if (q.count('tag') == 1):
        try:
            for dq in q.listen_item('tag'):
                x = ( 1 / 0 )            # <= Error 1
        except:
            pass
        if (q.list('tag')[0][4] == 1):
            try:
                for dq in q.listen_item('tag'):
                    x = ( 1 / 0 )        # <= Error 2
            except:
                pass
            if (q.list('tag')[0][4] == 2):
                q.enqueue('tag', {'err': 'data2'})
                try:
                    for dq in q.listen_item('tag'):
                        err = (dq[2] != '{"err":"data2"}')
                        break;
                except:
                    pass
    if err or (q.count('tag') != 1):
        raise Exception("failed excepted_times_to_ignore_listen 1")
    else:
        print 'OK excepted_times_to_ignore_listen 1'


def dequeue_and_listen_item_timeout():
    err = False
    with q.dequeue('tag') as dq:
        err = (dq != {u'err': u'data2'})
    if err:
        raise Exception("failed dequeue_and_listen_item_timeout 1")
    else:
        print 'OK dequeue_and_listen_item_timeout 1'

    before = datetime.datetime.now()
    for itm in q.listen_item('tag', timeout=1):
        after = datetime.datetime.now()
        delta = after - before
        span = (delta.seconds + (delta.microseconds / 1000000.0))
        err = not (itm == None and 0.95 < span and span < 1.05)
        break
    if err:
        raise Exception("failed dequeue_and_listen_item_timeout 2")
    else:
        print 'OK dequeue_and_listen_item_timeout 2'


def main():
    global q
    dsn = None
    if len(sys.argv) < 2:
        print 'set dsn for first argument.'
        sys.exit(1)
    dsn = sys.argv[1]
    q = q4pg.QueueManager(dsn, table_name=gettable())
    try:
        create_table()
        enqueue()
        dequeue()
        dequeue_line()
        show_list()
        dequeue_transaction()
        dequeue_listen()
        dequeue_item_listen()
        count_item()
        cancel()
        excepted_times_to_ignore()
        excepted_times_to_ignore_listen()
        dequeue_and_listen_item_timeout()
    except:
        raise
    finally:
        drop_table()


if __name__=="__main__":
    main()
