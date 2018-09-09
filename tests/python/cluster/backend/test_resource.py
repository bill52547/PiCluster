from dxpy.time.timestamps import TaskStamp
from dxpy.time.utils import strp

from dxl.cluster.backend import resource
from dxl.cluster.backend.resource import *
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database

data = {
	'source_CPU': 1,
	'source_GPU': 2,
	'source_memory': 3
}
update_data = {
	'source_CPU': 4,
	'source_GPU': 5,
	'source_memory': 6
}


def test_Resource():
	t = resource.Resource(**data)

	assert t.cpu_source == data['source_CPU']
	assert t.gpu_source == data['source_GPU']
	assert t.mem_source == data['source_memory']

	t2 = t.update_CPU(update_data['source_CPU'])
	assert t2.cpu_source == update_data['source_CPU']

	t2 = t.update_GPU(update_data['source_GPU'])
	assert t2.gpu_source == update_data['source_GPU']

	t2 = t.update_MEM(update_data['source_memory'])
	assert t2.mem_source == update_data['source_memory']


def test_allocate_node():
	c['path'] = ':memory:'
	Database.create()

	def make_task():
		task = base.Task(desc='test', workdir='/tmp/test',
						 worker=base.Worker.MultiThreading,
						 ttype=base.Type.Regular,
						 state=base.State.Pending,
						 dependency=None,
						 father=None,
						 time_stamp=TaskStamp(create=strp(
							 "2017-09-22 12:57:44.036185")),
						 data={'sample': 42},
						 is_root=True,
						 info={
							 'job_id': 1,
							 'nodes': 0,
							 'node_list': [2, 3, 4, 5],
							 'GPUs': 1,
							 'args': None
						 }
						 )

		task = web.Request().create(task)
		return task

	task1 = make_task()
	result_task = allocate_node(task1)
	task_json = result_task.to_json()
	task1_json = task1.to_json()
	assert task_json == task1_json

	for x in range(10):
		make_task()
	task2 = make_task()
	result_task2 = allocate_node(task2)
	assert result_task2 == None

	web.Request().delete(task1.id)
	web.Request().delete(task2.id)

	Database.clear()
	c.back_to_default()
