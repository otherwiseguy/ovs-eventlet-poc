import eventlet
eventlet.monkey_patch()

import os
import time

from ovs.db import idl
from ovs import poller
from ovsdbapp.backend.ovs_idl.idlutils import row2str

class MyIdl(idl.Idl):
    def notify(self, event, row, old=None):
        #print(event, row2str(row), row2str(old) if old else old)
        pass


class MyQueue(eventlet.queue.Queue):
    def __init__(self, *args, **kwargs):
        super(MyQueue, self).__init__(*args, **kwargs)
        alertpipe = os.pipe()
        self.alertin = os.fdopen(alertpipe[0], 'rb', 0)
        self.alertout = os.fdopen(alertpipe[1], 'wb', 0)

    def get_nowait(self, *args, **kwargs):
        result = super(MyQueue, self).get_nowait(*args, **kwargs)
        self.alertin.read(1)
        return result

    def put(self, *args, **kwargs):
        super(MyQueue, self).put(*args, **kwargs)
        self.alertout.write(b'X')
        self.alertout.flush()

    @property
    def alert_fileno(self):
        return self.alertin.fileno()


class Command(object):
    def __init__(self, _idl, val):
        self.idl = _idl
        self.val = val
        self.txn = None
        self.has_run = False

    def reset(self):
        self.txn = self.has_run = None

    def run(self):
        # For demo, txn is single command, this would go on Tranaction obj
        # and the loop would iterate through commands on the txn
        if not self.txn:
            self.has_run = False
            self.txn = idl.Transaction(i)
        if self.has_run:
            return
        row = self.txn.insert(self.idl.tables['Bridge'])
        row.name = "testbr%d" % self.val
        next(iter(self.idl.tables['Open_vSwitch'].rows.values())).addvalue('bridges', row)
        self.has_run = True


def run(_idl, q, ev):
    print("running")
    last_time = time.time()
    while True:
        p = poller.Poller()
        _idl.wait(p)
        p.fd_wait(q.alert_fileno, poller.POLLIN)
        p.block()

        now = time.time()
        stat = _idl.run()
        print(now - last_time, "CHANGED" if stat else "NO CHANGE")
        last_time = now

        # Notify client that we have connected and received the initial db
        if not ev.ready() and stat:
            print("SENDING EVENT")
            ev.send(True)

        try:
            # For demo, a txn is one cmd. txn obj would just loop over multiple
            c = q.get_nowait()
            c.run()
            status = c.txn.commit()
            print("STATUS %d %s" % (c.val, status))
            if status is c.txn.INCOMPLETE:
                # still have work to do, put it back on the queue
                print("Putting back", c.val)
                q.put(c)
            elif status is c.txn.TRY_AGAIN:
                # Row changed and failed verify(), reset and try again
                c.reset()
                q.put(c)
        except eventlet.queue.Empty:
            print("EMPTY")
            pass
        except Exception as ex:
            print("NOOOOOO!", ex)
            raise


def client(_idl, q, ev):
    print("waiting")
    ev.wait()
    print("GO!")
    for n in range(1000):
        q.put(Command(_idl, n))


#remote = 'tcp:127.0.0.1:6640'
remote = 'unix:/usr/local/var/run/openvswitch/db.sock'
schema_helper = idl.SchemaHelper(location="/usr/local/share/openvswitch/vswitch.ovsschema")
schema_helper.register_all()
i = MyIdl(remote, schema_helper)

q = MyQueue()
e = eventlet.Event()

pool = eventlet.GreenPool()
gt1 = pool.spawn(run, i, q, e)
gt2 = pool.spawn(client, i, q, e)
pool.waitall()
