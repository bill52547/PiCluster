import copy
import json
import unittest
import pytest
from dxl.cluster.database import base
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
from dxl.cluster.database.base import DBprocess, TaskDB, check_json

data = {
	"__task__": True,
	"desc": "a new recon task",
	"data": {
		"filename": "new.h5"
	},
	"state": "submit",
	"workdir": "/home/twj2417/Destop",
	"worker": "1",
	"father": [1],
	"type": "float",
	"dependency": ["task1", "task2"],
	"time_stamp": {
		"create": "2018-05-24 11:55:41.6",
		"start": None,
		"end": None
	},
	"is_root": False,
	"script_file": [],
	"info": {}
}
newdata = {
	"__task__": True,
	"id": 1,
	"desc": "a new recon task",
	"data": {
		"filename": "new.h5"
	},
	"state": "submit",
	"workdir": "/home/twj2417/Destop",
	"worker": "1",
	"father": [1],
	"type": "float",
	"dependency": ["task1", "task2"],
	"time_stamp": {
		"create": "2018-05-24 11:55:41.600000",
		"start": "2018-05-24 11:56:12.300000",
		"end": "2018-05-26 11:59:23.600000"
	},
	"is_root": False,
	"script_file": ["file.exe"],
	"info": {"job_id": 5826,
			 "partition": "main",
			 "name": "run.sh",
			 "user": "root",
			 "status": "PD",
			 "time": "0:00",
			 "nodes": 1,
			 "node_list": "(None)"
			 }
}

noupdate_result_data = {
	"__task__": True,
	"id": 1,
	"desc": "a new recon task",
	"data": {
		"filename": "new.h5"
	},
	"worker": "1",
	"type": "float",
	"workdir": "/home/twj2417/Destop",
	"dependency": ["task1", "task2"],
	"father": [1],
	"time_stamp": {
		"create": "2018-05-24 11:55:41.600000",
		"start": None,
		"end": None
	},
	"state": "submit",
	"is_root": False,
	"script_file": [],
	"info": {}
}


def test_create_session():
	DBprocess.create_session()
	assert DBprocess.session is not None


def test_get_or_create_session1():
	assert DBprocess.get_or_create_session() is not None


def test_get_or_create_session2():
	DBprocess.create_session()
	assert DBprocess.get_or_create_session() == DBprocess.session


def test_clear_session1():
	DBprocess.clear_session()
	assert DBprocess.session is None


def test_clear_session2():
	DBprocess.create_session()
	DBprocess.clear_session()
	assert DBprocess.session is None


class TestDatabase(unittest.TestCase):

	def setUp(self):
		c['path'] = ':memory:'
		Database.create()
		indata = json.dumps(data)
		base.DBprocess.create_session()
		self.tid = base.DBprocess.create(indata)

	def tearDown(self):
		Database.clear()
		c.back_to_default()
		base.DBprocess.clear_session()

	def test_create(self):
		assert self.tid == 1

	def test_delete(self):
		DBprocess.delete(self.tid)
		task_db = DBprocess.get_or_create_session().query(TaskDB).get(self.tid)
		assert task_db is None

	# TODO(hongjiang)
	# def test_readall(self):
	# 	task_jsons = []
	# 	DBprocess.read_all().subscribe(lambda t: task_jsons.append(t))
	# 	newresult = json.dumps(task_jsons)
	# 	print('*'*90)
	# 	print(newresult)
	# 	assert newresult == '["{\"__task__\": true, \"id\": 1, \"desc\": \"a new recon task\", \"data\": {\"filename\": \"new.h5\"}, \"worker\": \"1\", \"type\": \"float\", \"workdir\": \"/home/twj2417/Destop\", \"dependency\": [\"task1\", \"task2\"], \"father\": [1], \"time_stamp\": {\"create\": \"2018-05-24 11:55:41.600000\", \"start\": null, \"end\": null}, \"state\": \"submit\", \"is_root\": false, \"script_file\": [], \"info\": {}}"]'

	def test_noupdate_db2json_read(self):
		data1 = DBprocess.db2json(DBprocess.read_taskdb(self.tid))
		result1 = json.loads(data1)
		data2 = DBprocess.read(self.tid)
		result2 = json.loads(data2)
		assert result1 == noupdate_result_data
		assert result2 == noupdate_result_data

	def test_update_and_read(self):
		tid = newdata['id']
		indata = json.dumps(newdata)
		base.DBprocess.update(indata)
		data = base.DBprocess.read(tid)
		result = json.loads(data)
		assert result == newdata

	def test_check_json(self):
		newdata1 = copy.deepcopy(newdata)
		newdata2 = copy.deepcopy(newdata)
		newdata3 = copy.deepcopy(newdata)
		del newdata1['__task__']
		del newdata2['desc']
		newdata3['worker'] = eval(newdata3['worker'])
		indata1 = json.dumps(newdata1)
		indata2 = json.dumps(newdata2)
		indata3 = json.dumps(newdata3)
		try:
			check_json(indata1)
		except Exception as e:
			e = str(e)
			assert e == """'__task__' not found or is False for JSON: {"id": 1, "desc": "a new recon task", "data": {"filename": "new.h5"}, "state": "submit", "workdir": "/home/twj2417/Destop", "worker": "1", "father": [1], "type": "float", "dependency": ["task1", "task2"], "time_stamp": {"create": "2018-05-24 11:55:41.600000", "start": "2018-05-24 11:56:12.300000", "end": "2018-05-26 11:59:23.600000"}, "is_root": false, "script_file": ["file.exe"], "info": {"job_id": 5826, "partition": "main", "name": "run.sh", "user": "root", "status": "PD", "time": "0:00", "nodes": 1, "node_list": "(None)"}}"""
		try:
			check_json(indata2)
		except Exception as e:
			e = str(e)
			assert e == """Required key: desc is not found in JSON string: {"__task__": true, "id": 1, "data": {"filename": "new.h5"}, "state": "submit", "workdir": "/home/twj2417/Destop", "worker": "1", "father": [1], "type": "float", "dependency": ["task1", "task2"], "time_stamp": {"create": "2018-05-24 11:55:41.600000", "start": "2018-05-24 11:56:12.300000", "end": "2018-05-26 11:59:23.600000"}, "is_root": false, "script_file": ["file.exe"], "info": {"job_id": 5826, "partition": "main", "name": "run.sh", "user": "root", "status": "PD", "time": "0:00", "nodes": 1, "node_list": "(None)"}}"""
		try:
			check_json(indata3)
		except Exception as e:
			e = str(e)
			assert e == """Wrong type for key: worker with value: 1"""
