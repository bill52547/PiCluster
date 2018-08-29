from dxl.cluster.database.model import Database
from dxl.cluster.config import config as c
import json
import unittest
import requests


class TestWeb(unittest.TestCase):
	def setUp(self):
		c['path'] = ':memory:'
		Database.create()
		with open('/home/hongjiang/Desktop/dxcluster_config/data.json') as fin:
			data = json.load(fin)
		indata = json.dumps(data)
		result = requests.post(self.tasks_url(), {'task': indata}).json()
		self.tid = result['id']

	def tearDown(self):
		requests.delete(self.task_url(self.tid))
		Database.clear()
		c.back_to_default()

	def test_delete(self):
		requests.delete(self.task_url(self.tid))
		result = requests.get(self.tasks_url()).text
		assert result == '[]'

	def test_read_and_update(self):
		with open('/home/hongjiang/Desktop/dxcluster_config/newdata.json') as fin:
			inputs = json.load(fin)
		tid = inputs['id']
		indata = json.dumps(inputs)
		result = requests.put(self.tasks_url(), {'task': indata})
		data = requests.get(self.task_url(tid)).text
		result1 = json.loads(data)
		print(result1)
		assert result1 == {'__task__': True, 'id': 1, 'desc': 'a new recon task', 'data': {'filename': 'new.h5'},
						   'worker': '1', 'type': 'float', 'workdir': '/home/twj2417/Destop',
						   'dependency': ['task1', 'task2'], 'father': [1],
						   'time_stamp': {'create': '2018-05-24 11:55:41.600000', 'start': '2018-05-24 11:56:12.300000',
										  'end': '2018-05-26 11:59:23.600000'}, 'state': 'submit', 'is_root': False,
						   'script_file': ['file.exe'],
						   'info': {'job_id': 5826, 'partition': 'main', 'name': 'run.sh', 'user': 'root',
									'status': 'PD', 'time': '0:00', 'nodes': 1, 'node_list': '(None)'}}

	def tasks_url(self):
		return 'http://localhost:23300/api/v0.2/tasks'

	def task_url(self, tid):
		return 'http://localhost:23300/api/v0.2/task/{}'.format(tid)
