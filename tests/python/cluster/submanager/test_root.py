from dxl.cluster.submanager import base as rootbase
from dxl.cluster.interactive import base,web
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
from dxpy.time.timestamps import TaskStamp
from dxpy.time.utils import strp
import unittest
import rx


task = base.Task( desc='test', workdir='/tmp/test',
                      worker=base.Worker.MultiThreading,
                      ttype=base.Type.Regular,
                      state=base.State.Pending,
                      dependency=None,
                      father=None,
                      time_stamp=TaskStamp(create=strp(
                          "2017-09-22 12:57:44.036185")),
                      data={'sample': 42},
                      is_root=True)



class TestRoot(unittest.TestCase):
    def setUp(self):
        c['path'] = ':memory:'
        Database.create()
        self.t1 = web.Request().create(task)
        task1 = base.Task( desc='test', workdir='/tmp/test',
                      worker=base.Worker.MultiThreading,
                      ttype=base.Type.Regular,
                      state=base.State.Failed,
                      dependency=None,
                      father=[self.t1.id],
                      time_stamp=TaskStamp(create=strp(
                          "2017-09-22 12:57:44.036185")),
                      data={'sample': 42},
                      is_root=True)
        self.t2 = web.Request().create(task1)
def test_complete(self):
        result = rootbase.complete_rate(web.Request().read(self.t1.id))
        assertsk( desc='test', workdir='/tmp/test',
                      worker=base.Worker.MultiThreading,
                      ttype=base.Type.Regular,
                      state=base.State.Complete,
                      dependency=[self.t2.id],
                      father=[self.t1.id],
                      time_stamp=TaskStamp(create=strp(
                          "2017-09-22 12:57:44.036185")),
                      data={'sample': 42},
                      is_root=True)
        self.t3 = web.Request().create(task2)
    
    def tearDown(self):
        web.Request().delete(self.t1.id)
        web.Request().delete(self.t2.id)
        web.Request().delete(self.t3.id)
        Database.clear()
        c.back_to_default()

    def test_complete(self):
        result = rootbase.complete_rate(web.Request().read(self.t1.id))
        assert result == 0.5

    def test_resubmit(self):
        rootbase.resubmit_failure(web.Request().read(self.t1.id))
        tasknew = web.Request().read(self.t3.id)
        assert tasknew.dependency == [4]
        web.Request().delete(4)
        data = (web.Request().read_all().to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
        assert len(data) == 3
