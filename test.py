#!/usr/bin/env python
import os, sys, q4pg, time
from datetime import datetime, timedelta
from multiprocessing import Process, Queue

q = None

def getspan(before):
    after = datetime.now()
    delta = after - before
    return (delta.seconds + (delta.microseconds / 1000000.0))

def gettable():
    return 'test_table_%s' % str(datetime.now().microsecond).replace(' ', '')

def create_table():
    q.create_table()

def drop_table():
    q.drop_table()

def enqueue():
    _0 = q.enqueue('tag', {'the_data': 'must_be'})
    _1 = q.enqueue('tag', {'json': 'serializable_data'})
    _2 = q.enqueue('tag', {'more': 'data'})
    _3 = q.enqueue('tag', {'more': 'data2'})
    _4 = q.enqueue('tag', {'more': 'data3'})
    if len(q.list('tag')) != 5:
        raise Exception("failed enqueue 1")
    else:
        print('OK enqueue 1')
    # checking id
    ids = [_0, _1, _2, _3, _4]
    if ids != list(range(1, 6)):
        raise Exception("failed enqueue 2")
    else:
        print('OK enqueue 2')

def dequeue():
    with q.dequeue('tag') as dq:
        if dq != {'the_data': 'must_be'}:
            raise Exception("failed dequeue 1")
        else:
            print('OK dequeue 1')
    with q.dequeue('another_tag') as dq:
        if dq != None:
            raise Exception("failed dequeue 2")
        else:
            print('OK dequeue 2')

def dequeue_line():
    with q.dequeue_item('tag') as dq:
        if (not (dq[1] == 'tag' and dq[2] == u'{"json":"serializable_data"}')):
            raise Exception("failed dequeue line 1")
        else:
            print('OK dequeue line 1')
    with q.dequeue_item('another_tag') as dq:
        if dq != None:
            raise Exception("failed dequeue 2")
        else:
            print('OK dequeue line 2')

def show_list():
    lis = q.list('tag')
    if len(lis) != 3:
        raise Exception("failed list 1")
    else:
        print('OK list 1')

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
        print('OK dequeue_transaction 1')

def dequeue_listen():
    qs = []
    for i in q.listen('tag'):
        qs.append(i)
        if i == {'more': 'data3'}:
            break
    if qs != [{'more': 'data'},
              {'more': 'data2'},
              {'more': 'data3'}]:
        raise Exception("failed dequeue_listen 1")
    else:
        print('OK dequeue_listen 1')

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
        print('OK dequeue_item_listen 1')

def count_item():
    if q.count('tag') != 1:
        raise Exception("failed count_item 1")
    else:
        print('OK count_item 1')
    q.enqueue('tag', {'new': 'data2'})
    if q.count('tag') != 2:
        raise Exception("failed count_item 2")
    else:
        print('OK count_item 2')

def cancel():
    if ((not q.cancel(6)) or
        (not q.cancel(7)) or
        q.cancel(3) or
        q.count('tag') != 0):
        raise Exception("failed cancel 1")
    else:
        print('OK cancel 1')

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
                            print('OK excepted_times_to_ignore 1')
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
                        break
                except:
                    pass
    if err or (q.count('tag') != 1):
        raise Exception("failed excepted_times_to_ignore_listen 1")
    else:
        print('OK excepted_times_to_ignore_listen 1')

def dequeue_and_listen_item_timeout():
    err = False
    with q.dequeue('tag') as dq:
        err = (dq != {u'err': u'data2'})
    if err:
        raise Exception("failed dequeue_and_listen_item_timeout 1")
    else:
        print('OK dequeue_and_listen_item_timeout 1')

    timeout_sec = 3
    before = datetime.now()
    for itm in q.listen_item('tag', timeout=timeout_sec):
        span = getspan(before)
        err = not (itm == None and
                   (timeout_sec - 0.05) < span and
                   span < (timeout_sec + 0.05))
        break
    if err:
        raise Exception("failed dequeue_and_listen_item_timeout 2")
    else:
        print('OK dequeue_and_listen_item_timeout 2')

def dequeue_item_immediate():
    q.enqueue("tag_dii", {'id': 0})
    q.enqueue("tag_dii", {'id': 1})
    dq = q.dequeue_item_immediate("tag_dii")
    if dq[1] != 'tag_dii' or dq[2] != '{"id":0}':
        raise Exception("failed dequeue_item_immediate 1")
    else:
        print('OK dequeue_item_immediate 1')
    if len(q.list('tag_dii')) != 1:
        raise Exception("failed dequeue_item_immediate 2")
    else:
        print('OK dequeue_item_immediate 2')
    dq = q.dequeue_item_immediate("tag_dii")
    if dq[1] != 'tag_dii' or dq[2] != '{"id":1}':
        raise Exception("failed dequeue_item_immediate 3")
    else:
        print('OK dequeue_item_immediate 3')
    if len(q.list('tag_dii')) != 0:
        raise Exception("failed dequeue_item_immediate 4")
    else:
        print('OK dequeue_item_immediate 4')

def dequeue_transaction_none():
    idt = "test-exception"
    q.excepted_times_to_ignore = 1
    q.enqueue('tag', {'test':'test'})
    try:
        with q.dequeue('tag') as dq:
            x = ( 1 / 0 )                     # <= Error
    except:
        pass
    #queue is ignored dequeue will return None.
    try:
        with q.dequeue('tag') as dq:
            if dq == None:
                raise Exception(idt)
    except Exception as e:
        if str(e) == idt and q.list('tag')[0][4] <= 1:
            print('OK dequeue_transaction_none 2')
        else:
            raise Exception("failed dequeue_transaction_none 2")

def dangerous_data_sanitizing():
    try:
        q.enqueue("I'm", {})
    except ValueError as e:
        if str(e)[0:16] == "Invalid tag-name":
            print('OK dangerous_data_sanitizing 1')
        else:
            raise Exception("failed dangerous_data_sanitizing 1")
    data = {"'ah''basjd''f'f'kk'''a'ha": "uuu'xxx"}
    q.enqueue("thats", data)
    dq = q.dequeue_immediate("thats")
    if (dq == data):
        print('OK dangerous_data_sanitizing 2')
    else:
        raise Exception("failed dangerous_data_sanitizing 1")


def wait_until_convenient():
    while True:
        now = datetime.now()
        if now.microsecond < 100: break;
        time.sleep(50/1000000.0)

def scheduling():
    # basic.
    wait_until_convenient()
    now = datetime.now()
    s = now + timedelta(0, 2) # after 2 sec
    q.enqueue("scheduling", {'test': 'wait-2s'}, schedule = s)
    dq = q.dequeue_immediate("scheduling")
    if dq: raise Exception("failed scheduling 1")
    time.sleep(1)
    dq = q.dequeue_immediate("scheduling")
    if dq: raise Exception("failed scheduling 2")
    time.sleep(1.05)
    dq = q.dequeue_immediate("scheduling")
    if dq:
        print('OK scheduling 1')
    else:
        raise Exception("failed scheduling 3")
    # checking listen and order.
    wait_until_convenient()
    now = datetime.now()
    s = now + timedelta(0, 2) # after 2 sec
    src = [{'order': i} for i in range(10)]
    for i in src: q.enqueue("scheduling", i, schedule = s)
    res = []
    before = datetime.now()
    for dq in q.listen("scheduling"):
        if len(res) == 0:
            span = getspan(before)
            if (span < 1.90 or 2.1 < span):
                raise Exception("failed scheduling 4 ideal: 2, span:" + str(span))
        res.append(dq)
        if (len(res) == len(src)): break
    if (src == res):
        print('OK scheduling 2')
    else:
        raise Exception("failed scheduling 5")
    # checking preserving order when varied scheduled queue post in varied timing.
    wait_until_convenient()
    _0 = [{'order': 0},  {'order': 1},  {'order': 2}]
    _1 = [{'order': 3},  {'order': 4},  {'order': 5}]
    _2 = [{'order': 6},  {'order': 7},  {'order': 8}]
    _3 = [{'order': 9},  {'order': 10}, {'order': 11}]
    _4 = [{'order': 12}, {'order': 13}, {'order': 14}]
    res = []
    now = datetime.now()
    s = now + timedelta(0, 3) # after 2 sec
    for i in _3: q.enqueue("scheduling", i, schedule = s)
    s = now + timedelta(0, 1)
    for i in _0: q.enqueue("scheduling", i, schedule = s)
    before = datetime.now()
    for dq in q.listen("scheduling"):
        res.append([dq, getspan(before)])
        if (len(res) == len(_0)):
            s = datetime.now() + timedelta(0, 1)
            for i in _2: q.enqueue("scheduling", i, schedule = s)
            for i in _1: q.enqueue("scheduling", i)
            break
    for dq in q.listen("scheduling"):
        res.append([dq, getspan(before)])
        if (len(res) == (len(_0) * 2)):
            break
    for dq in q.listen("scheduling"):
        res.append([dq, getspan(before)])
        if (len(res) == (len(_0) * 3)):
            s = datetime.now() + timedelta(0, 2)
            for i in _4: q.enqueue("scheduling", i, schedule = s)
            break
    for dq in q.listen("scheduling"):
        res.append([dq, getspan(before)])
        if (len(res) == (len(_0) * 5)):
            break

    i = 0
    for s, r in zip((_0 + _1 + _2 + _3 + _4), res):
        if s != r[0]:
            raise Exception("failed scheduling 6 " + str(s) + ", " + str(r) + ", " + str(res))
        span = r[1]
        ideal = (1 if i < 6 else (int(i / 3) + 0.3))
        if span < (ideal - 0.3) or (ideal + 0.3) < span:
            raise Exception("failed scheduling 7 " + str(ideal) + ", " + str(span))
        i+=1
    print('OK scheduling 3')
    while True:
        i = q.dequeue_immediate("scheduling")
        if i == None: break
    # check list count
    now = datetime.now()
    s = now + timedelta(0, 1) # after 1 sec
    for i in _0: q.enqueue("scheduling", i, schedule = s)
    if (q.count("scheduling") > 0 or
        len(q.list("scheduling")) > 0):
        raise Exception("failed scheduling 8.")
    if (q.count("scheduling", ignore_scheduled = False) < 1 or
        len(q.list("scheduling", ignore_scheduled = False)) < 1):
        raise Exception("failed scheduling 9.")
    time.sleep(1)
    if (q.count("scheduling") < 1 or
        len(q.list("scheduling")) < 1):
        raise Exception("failed scheduling 10.")
    print('OK scheduling 4')

def enqueue2():
    _0 = q.enqueue('tag', {'int_param': 1})
    print('OK enqueue2 1')
    _0 = q.enqueue('tag', {'float_param': 0.01})
    print('OK enqueue2 2')

def multiprocess_tasks():
    wait_until_convenient()
    TAG = "message_q"
    def fetch_task(queue):
        pid = os.getpid()
        count = 0
        for dq in q.listen(TAG, timeout=10):
            s = { 'pid': pid, 'data': dq }
            if dq:
                count += 1
                queue.put(s)

            if dq == None:
                break
        return count

    test_items = range(1, 300)
    for i in test_items:
        q.enqueue(TAG, i)

    jobs = []
    wait_until_convenient()
    queue = Queue()
    for i in range(1, 6):
        job = Process(target=fetch_task, args=(queue,))
        jobs.append(job)
        job.start()
    [j.join() for j in jobs]

    processed_data = set()
    qsize = queue.qsize()
    while not queue.empty():
        item = queue.get()
        data = item.get('data')
        if data in processed_data:
            raise Exception("failed multiprocess_tasks - data %s has been processed already" % (data, ))
        processed_data.add(item.get('data'))

    if qsize == len(test_items):
        print("OK multiprocess_tasks")
    else:
        raise Exception("failed multiprocess_tasks - tasks are not complete")

def main():
    global q
    dsn = None
    if len(sys.argv) < 2:
        print('set dsn for first argument.')
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
        dequeue_item_immediate()
        dequeue_transaction_none()
        dangerous_data_sanitizing()
        scheduling()
        enqueue2()
        multiprocess_tasks()
    except:
        raise
    finally:
        drop_table()


if __name__=="__main__":
    main()
