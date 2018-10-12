import time

from dxl.cluster.time.timestamps import TaskStamp
from dxl.cluster.time.utils import strp

from dxl.cluster.database.base import DBprocess
from dxl.cluster.database.model import Database
from dxl.cluster.interactive import base, web
from dxl.cluster.config import config as c
from dxl.cluster.taskguard import cycle
import unittest


class TestCycle(unittest.TestCase):
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
                               father=None,
                               time_stamp=TaskStamp(create=strp(
                                   "2017-09-22 12:57:44.036185")),
                               data={'sample': 42},
                               is_root=True)
        self.task2 = web.Request().create(self.task2)

        self.task3 = base.Task(desc='test', workdir='/tmp/test',
                               worker=base.Worker.MultiThreading,
                               ttype=base.Type.Regular,
                               state=base.State.Pending,
                               dependency=None,
                               father=None,
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
        Database.clear()
        c.back_to_default()

    def test_cycle(self):
        time.sleep(15)
        tasks = web.Request().read_all().to_list().to_blocking().first()
        for task in tasks:
            assert task.state == base.State.Complete

    def test_get_graph_task(self):
        assert cycle.get_graph_task() is not None

    def test_get_nodes(self):
        assert cycle.get_nodes() == [1, 2, 3]

    def test_get_depends(self):
        assert cycle.get_depens() == [[], [], []]
