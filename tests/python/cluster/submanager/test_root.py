from dxl.cluster.database.base import DBprocess
from dxl.cluster.submanager import base as rootbase
from dxl.cluster.interactive import base, web
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
from dxpy.time.timestamps import TaskStamp
from dxpy.time.utils import strp
import unittest
import rx


class TestRoot(unittest.TestCase):
    def setUp(self):
        # Database.clear()
        c['path'] = ':memory:'
        Database.create()
        self.task1 = base.Task(desc='test', workdir='/tmp/test',
                               worker=base.Worker.MultiThreading,
                               ttype=base.Type.Regular,
                               state=base.State.Pending,
                               dependency=None,
                               father=None,
                               time_stamp=TaskStamp(create=strp(
                                   "2017-09-22 12:57:44.036185")),
                               data={'sample': 42},
                               is_root=True)
        self.task1 = web.Request().create(self.task1)

        self.task2 = base.Task(desc='test', workdir='/tmp/test',
                               worker=base.Worker.MultiThreading,
                               ttype=base.Type.Regular,
                               state=base.State.Pending,
                               dependency=None,
                               father=[self.task1.id],
                               time_stamp=TaskStamp(create=strp(
                                   "2017-09-22 12:57:44.036185")),
                               data={'sample': 42},
                               is_root=True)
        self.task2 = web.Request().create(self.task2)

        self.task3 = base.Task(desc='test', workdir='/tmp/test',
                               worker=base.Worker.MultiThreading,
                               ttype=base.Type.Regular,
                               state=base.State.Failed,
                               dependency=None,
                               father=[self.task1.id],
                               time_stamp=TaskStamp(create=strp(
                                   "2017-09-22 12:57:44.036185")),
                               data={'sample': 42},
                               is_root=True)
        self.task3 = web.Request().create(self.task3)

    def tearDown(self):
        web.Request().delete(self.task1.id)
        web.Request().delete(self.task2.id)
        web.Request().delete(self.task3.id)

        DBprocess.clear_session()
        c.back_to_default()

    def test_num_subs(self):
        assert rootbase.num_subs(self.task1) == 2

    def test_complete_rate(self):
        assert rootbase.is_completed(self.task1) == 0

    def test_fail_rate(self):
        assert rootbase.is_failed(self.task1) == 0.5

    def test_resubmit_failure(self):
        self.task4 = base.Task(desc='test', workdir='/tmp/test',
                               worker=base.Worker.MultiThreading,
                               ttype=base.Type.Regular,
                               state=base.State.Pending,
                               dependency=[self.task3.id],
                               father=[self.task1.id],
                               time_stamp=TaskStamp(create=strp(
                                   "2017-09-22 12:57:44.036185")),
                               data={'sample': 42},
                               is_root=True)
        self.task4 = web.Request().create(self.task4)

        rootbase.resubmit_failure(self.task1)
        self.task5 = web.Request().read(self.task4.id + 1)
        assert self.task5.id == self.task4.id + 1
        self.task4_again = web.Request().read(self.task4.id)
        assert self.task4_again.dependency == [self.task5.id]
