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
		Database.clear()
		c['path'] = ':memory:'
		Database.create()
		self.task = base.Task(desc='test', workdir='/tmp/test',
							  worker=base.Worker.MultiThreading,
							  ttype=base.Type.Regular,
							  state=base.State.Pending,
							  dependency=None,
							  father=None,
							  time_stamp=TaskStamp(create=strp(
								  "2017-09-22 12:57:44.036185")),
							  data={'sample': 42},
							  is_root=True)
		self.task = web.Request().create(self.task)

		self.task1 = base.Task(desc='test', workdir='/tmp/test',
							   worker=base.Worker.MultiThreading,
							   ttype=base.Type.Regular,
							   state=base.State.Pending,
							   dependency=None,
							   father=[self.task.id],
							   time_stamp=TaskStamp(create=strp(
								   "2017-09-22 12:57:44.036185")),
							   data={'sample': 42},
							   is_root=True)
		self.task1 = web.Request().create(self.task1)

		self.task2 = base.Task(desc='test', workdir='/tmp/test',
							   worker=base.Worker.MultiThreading,
							   ttype=base.Type.Regular,
							   state=base.State.Failed,
							   dependency=None,
							   father=[self.task.id],
							   time_stamp=TaskStamp(create=strp(
								   "2017-09-22 12:57:44.036185")),
							   data={'sample': 42},
							   is_root=True)
		self.task2 = web.Request().create(self.task2)

	def tearDown(self):
		Database.clear()
		c.back_to_default()

	def test_num_subs(self):
		assert rootbase.num_subs(self.task) == 2

	def test_complete_rate(self):
		assert rootbase.complete_rate(self.task) == 0

	def test_fail_rate(self):
		assert rootbase.fail_rate(self.task) == 0.5

	def test_resubmit_failure(self):
		self.task3 = base.Task(desc='test', workdir='/tmp/test',
							   worker=base.Worker.MultiThreading,
							   ttype=base.Type.Regular,
							   state=base.State.Failed,
							   dependency=[self.task2.id],
							   father=[self.task.id],
							   time_stamp=TaskStamp(create=strp(
								   "2017-09-22 12:57:44.036185")),
							   data={'sample': 42},
							   is_root=True)
		self.task3 = web.Request().create(self.task3)

		rootbase.resubmit_failure(self.task)
		assert web.Request().read(self.task3.id + 1) is not None

		self.task3 = web.Request().read(self.task3.id)
		assert self.task3.dependency[0] == self.task3.id + 1
