import requests
from dxl.cluster.interactive import base
from dxl.cluster.interactive import web
import unittest
import json
from dxl.cluster.time.timestamps import TaskStamp
from dxl.cluster.time.utils import strp
import rx
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
from dxl.cluster.backend import slurm
from dxl.cluster.interactive.web import connection_error_handle


def test_connection_error_handle():
    @connection_error_handle
    def func1():
        try:
            a = 1 / 0
        except Exception as e:
            raise requests.ConnectionError(e)

    try:
        result = func1()
    except Exception as e:
        assert str(e) == "Task database server connection failed. Details:\n{e}".format(e='division by zero')


task = slurm.TaskSlurm(['run.sh'], workdir='/mnt/gluster/twj/GATE/16_8.0/sub.0', desc='father', is_root=True,
                       ttype=base.Type.Script,
                       time_stamp=TaskStamp(
                           create=strp("2017-09-22 12:57:44.036185"),
                           start=strp("2018-05-24 11:56:12.300000"),
                           end=strp("2018-05-26 11:59:23.600000")))


class Testweb(unittest.TestCase):
    def setUp(self):
        c['path'] = ':memory:'
        Database.create()
        self.t1 = web.Request().create(task)

    def tearDown(self):
        web.Request().delete(self.t1.id)
        Database.clear()
        c.back_to_default()

    def test_readall(self):
        result = (web.Request().read_all().to_list()
                  .subscribe_on(rx.concurrency.ThreadPoolScheduler())
                  .to_blocking().first())
        assert result[0].state == base.State.BeforeSubmit

    def test_read(self):
        result = web.Request().read(self.t1.id)
        assert result.state == base.State.BeforeSubmit

    def test_submit(self):
        t = web.submit(self.t1)
        t2 = web.Request().read(self.t1.id)
        assert t2.state == t.state
        assert t2.state == base.State.Pending

    def define_task(self):
        return base.Task(tid=self.t1.id, script_file=['run1.sh'], workdir='/mnt/gluster/twj/GATE/16_8.0/sub.0',
                         desc='father', is_root=True,
                         ttype=base.Type.Script,
                         time_stamp=TaskStamp(
                             create=strp("2017-09-22 12:57:44.036185"),
                             start=strp("2018-05-24 11:56:12.300000"),
                             end=strp("2018-05-26 11:59:23.600000")))

    def test_update(self):
        task1 = self.define_task()
        web.Request().update(task1)
        t = web.Request().read(self.t1.id)
        assert t.script_file == ['run1.sh']

    def test_start(self):
        t = web.start(self.t1)
        t2 = web.Request().read(self.t1.id)
        assert t2.state == t.state
        assert t2.state == base.State.Runing

    def test_complete(self):
        t = web.complete(self.t1)
        t2 = web.Request().read(self.t1.id)
        assert t2.state == t.state
        assert t2.state == base.State.Complete
