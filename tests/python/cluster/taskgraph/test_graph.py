from dxpy.time.timestamps import TaskStamp

from dxl.cluster.interactive.run import TaskSleep
from dxl.cluster.interactive.base import State, Worker, Type, Task
from dxl.cluster.taskgraph.base import Graph
import unittest
from dxpy.time.utils import now, strp
from dxl.cluster.interactive import web, fortest
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
from dxl.cluster.interactive import base

t1 = TaskSleep(tid=1, desc='sleep 10 s', workdir='.', worker=Worker.Local, ttype=Type.Regular,
			   state=State.BeforeSubmit, time_stamp=now, dependency=None, is_root=True, data=None)
t2 = TaskSleep(tid=2, desc='sleep 10 s', workdir='.', worker=Worker.Local, ttype=Type.Regular,
			   state=State.BeforeSubmit, time_stamp=now, dependency=[t1], is_root=True, data=None)

task1 = Task(tid=1, desc='test', workdir='/tmp/test',
			 worker=base.Worker.MultiThreading,
			 ttype=base.Type.Regular,
			 state=base.State.Complete,
			 dependency=None,
			 father=None,
			 time_stamp=TaskStamp(create=strp(
				 "2017-09-22 12:57:44.036185")),
			 data={'sample': 42},
			 is_root=True)

task2 = Task(tid=2, desc='test', workdir='/tmp/test',
			 worker=base.Worker.MultiThreading,
			 ttype=base.Type.Regular,
			 state=base.State.Pending,
			 dependency=[1],
			 father=None,
			 time_stamp=TaskStamp(create=strp(
				 "2017-09-22 12:57:44.036185")),
			 data={'sample': 42},
			 is_root=True)


class TestGraph(unittest.TestCase):
	def setUp(self):
		Database.clear()
		c['path'] = ':memory:'
		Database.create()

	def test_all_runable(self):
		g = Graph([t1.id, t2.id], [None, t1.id])
		t = g.all_runable()
		assert t == [1]

	def test_mark(self):
		t1 = web.Request().create(task1)
		t2 = web.Request().create(task2)
		g = Graph([t1.id, t2.id], [t1.dependency, t2.dependency])
		assert list(g.nodes()) == [1, 2]
		g.mark_complete()
		assert list(g.nodes()) == [2]
