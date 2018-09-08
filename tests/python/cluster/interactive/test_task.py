import copy
import unittest
import json
from dxl.cluster.interactive import base
from dxpy.time.timestamps import TaskStamp
from dxpy.time.utils import strp

from dxl.cluster.interactive.base import State, TaskInfo

dct = {
	'__task__': True,
	'id': 10,
	'desc': 'test',
	'workdir': '/tmp/test',
	'worker': 'Slurm',
	'type': 'Script',
	'dependency': [1, 2, 3],
	'father': [1],
	'data': {'sample': 42},
	'is_root': True,
	'time_stamp': {
		'create': "2017-09-22 12:57:44.036185",
		'start': None,
		'end': None
	},
	'state': 'BeforeSubmit',
	'script_file': ['file1.txt', 'file2.csv', 'file3.pdf'],
	'info': {}
}

time_stamp = "2017-09-22 12:57:44.036185"
dct2 = {'tid': 10, 'desc': 'test', 'workdir': '/tmp/test',
		'worker': base.Worker.MultiThreading,
		'ttype': base.Type.Regular,
		'dependency': [1, 2, 3],
		'father': [1],
		'time_stamp': TaskStamp(create=strp(time_stamp)),
		'data': {'sample': 42},
		'is_root': True}
dct3 = {
	'sid': 1,
	'nb_nodes': 0,
	'node_list': [2, 3, 4, 5],
	'nb_GPU': None,
	'args': None
}
parse_dct = {
	'job_id': 1,
	'nodes': 0,
	'node_list': [2, 3, 4, 5],
	'GPUs': None,
	'args': None
}


def test_TaskInfo_init():
	t = TaskInfo(**dct3)

	assert t.sid == dct3['sid']
	if dct3['nb_nodes'] == None:
		assert t.nb_nodes == 0
	else:
		assert t.nb_nodes == dct3['nb_nodes']

	assert t.node_list == dct3['node_list']
	if dct3['nb_GPU'] == None:
		assert t.nb_GPU == 0
	else:
		assert t.nb_GPU == dct3['nb_GPU']
	if dct3['args'] == None:
		assert t.args == ''
	else:
		assert t.args == dct3['args']


def test_parse_dict():
	t = TaskInfo.parse_dict(parse_dct)
	assert t.sid == parse_dct['job_id']
	if parse_dct['nodes'] == None:
		assert t.nb_nodes == 0
	else:
		assert t.nb_nodes == parse_dct['nodes']

	assert t.node_list == parse_dct['node_list']
	if parse_dct['GPUs'] == None:
		assert t.nb_GPU == 0
	else:
		assert t.nb_GPU == parse_dct['GPUs']
	if parse_dct['args'] == None:
		assert t.args == ''
	else:
		assert t.args == parse_dct['args']


def test_to_dict():
	t = TaskInfo(**dct3)

	if parse_dct['nodes'] == None:
		parse_dct['nodes'] = 0
	if parse_dct['GPUs'] == None:
		parse_dct['GPUs'] = 0
	if parse_dct['args'] == None:
		parse_dct['args'] = ''
	result = t.to_dict()
	assert result == parse_dct

def test_update_node_list():
	node_list = [1,2,3]
	t = TaskInfo(**dct3)
	t2 = t.update_node_list(node_list)

	assert t2.sid == dct3['sid']
	if dct3['nb_nodes'] == None:
		assert t2.nb_nodes == 0
	else:
		assert t2.nb_nodes == dct3['nb_nodes']

	assert t2.node_list == node_list
	if dct3['nb_GPU'] == None:
		assert t2.nb_GPU == 0
	else:
		assert t2.nb_GPU == dct3['nb_GPU']
	if dct3['args'] == None:
		assert t2.args == ''
	else:
		assert t2.args == dct3['args']


def test_update_args():
	args = 1
	t = TaskInfo(**dct3)
	t2 = t.update_args(args)

	assert t2.sid == dct3['sid']
	if dct3['nb_nodes'] == None:
		assert t2.nb_nodes == 0
	else:
		assert t2.nb_nodes == dct3['nb_nodes']

	assert t2.node_list == dct3['node_list']
	if dct3['nb_GPU'] == None:
		assert t2.nb_GPU == 0
	else:
		assert t2.nb_GPU == dct3['nb_GPU']
	if args == None:
		assert t2.args == ''
	else:
		assert t2.args == args


class Testtask(unittest.TestCase):
	def test_to_json(self):
		t = base.Task(**dct2)
		s = t.to_json()
		dct = json.loads(s)
		self.assertEqual(dct['id'], dct2['tid'])
		self.assertEqual(dct['father'], dct2['father'])
		self.assertEqual(dct['desc'], dct2['desc'])
		self.assertEqual(dct['dependency'], dct2['dependency'])
		self.assertEqual(dct['data'], dct2['data'])
		self.assertEqual(dct['type'], dct2['ttype'].name)
		self.assertEqual(dct['workdir'], dct2['workdir'])
		self.assertEqual(dct['worker'], dct2['worker'].name)
		self.assertEqual(dct['is_root'], dct2['is_root'])
		self.assertEqual(dct['time_stamp'], {
			'create': time_stamp, 'start': 'None', 'end': 'None'})
		self.assertEqual(dct['state'], State.BeforeSubmit.name)

	def test_from_json(self):
		t = base.Task.from_json(json.dumps(dct))
		# t2 = base.Task.from_json(json.dumps(dct))
		self.assertEqual(t.id, dct['id'])
		self.assertEqual(t.desc, dct['desc'])
		self.assertEqual(t.workdir, dct['workdir'])
		self.assertEqual(t.worker, base.Worker.Slurm)
		self.assertEqual(t.type, base.Type.Script)
		self.assertEqual(t.dependency, dct['dependency'])
		self.assertEqual(t.father, dct['father'])
		self.assertEqual(t.data, dct['data'])
		self.assertEqual(t.is_root, dct['is_root'])
		self.assertEqual(t.time_stamp.create, strp(dct['time_stamp']['create']))
		self.assertEqual(t.state, base.State.BeforeSubmit)
		self.assertEqual(t.script_file, dct['script_file'])
		self.assertEqual(t.info, dct['info'])
		# assert t1 == t2
	def test_init(self):
		t = base.Task.from_json(json.dumps(dct))
		if dct['state'] == 'BeforeSubmit':
			assert t.is_before_submit == True
			assert t.is_pending == False
			assert t.is_running == False
			assert t.is_complete == False
			assert t.is_fail == False
		if dct['state'] == 'Pending':
			assert t.is_before_submit == False
			assert t.is_pending == True
			assert t.is_running == False
			assert t.is_complete == False
			assert t.is_fail == False
		if dct['state'] == 'Runing':
			assert t.is_before_submit == False
			assert t.is_pending == False
			assert t.is_running == True
			assert t.is_complete == False
			assert t.is_fail == False
		if dct['state'] == 'Complete':
			assert t.is_before_submit == False
			assert t.is_pending == False
			assert t.is_running == False
			assert t.is_complete == True
			assert t.is_fail == False
		if dct['state'] == 'Failed':
			assert t.is_before_submit == False
			assert t.is_pending == False
			assert t.is_running == False
			assert t.is_complete == False
			assert t.is_fail == True

	def test_replce_dependency(self):
		t = base.Task.from_json(json.dumps(dct))
		t.replace_dependency(2, 4)
		assert t.dependency == [1, 4, 3]

	def test_update_state(self):
		t = base.Task.from_json(json.dumps(dct))
		t2 = t.update_state(base.State.Pending)
		assert t2.state == base.State.Pending

	def test_info(self):
		t = base.Task.from_json(json.dumps(dct))
		new_info = {
			"job_id": 5826,
			"partition": "main",
			"name": "run.sh",
			"user": "root",
			"status": "PD",
			"time": "0:00",
			"nodes": 1,
			"node_list": "(None)"
		}
		t2 = t.update_info(new_info)
		assert t2.info == new_info
