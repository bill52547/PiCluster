import copy
import json
import unittest
import pytest
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
from dxl.cluster.database.base import DBprocess, TaskDB, check_json
from .data_for_test import *


@pytest.fixture()
def before():
	DBprocess.session = None


def test_create_session(before):
	DBprocess.create_session()
	assert DBprocess.session is not None


def test_get_or_create_session1(before):
	assert DBprocess.get_or_create_session() is not None


def test_get_or_create_session2(before):
	DBprocess.create_session()
	assert DBprocess.get_or_create_session() == DBprocess.session


def test_clear_session1(before):
	DBprocess.clear_session()
	assert DBprocess.session is None


def test_clear_session2(before):
	DBprocess.create_session()
	DBprocess.clear_session()
	assert DBprocess.session is None


class TestDatabase(unittest.TestCase):

	def setUp(self):
		c['path'] = ':memory:'
		Database.create()
		indata = json.dumps(data)
		DBprocess.create_session()
		self.tid = DBprocess.create(indata)

	def tearDown(self):
		Database.clear()
		c.back_to_default()
		DBprocess.clear_session()

	def test_create(self):
		assert DBprocess.get_or_create_session().query(TaskDB).get(self.tid).desc == data['desc']
		assert self.tid == DBprocess.get_or_create_session().query(TaskDB).get(self.tid).id

	def test_delete(self):
		DBprocess.delete(self.tid)
		task_db = DBprocess.get_or_create_session().query(TaskDB).get(self.tid)
		assert task_db is None

	def test_readall(self):
		task_jsons = []
		DBprocess.read_all().subscribe(lambda t: task_jsons.append(t))
		newresult = json.dumps(task_jsons)
		self.assertIsNotNone(newresult)

	def test_noupdate_db2json(self):
		data = DBprocess.db2json(DBprocess.read_taskdb(self.tid))
		result = json.loads(data)
		assert result == noupdate_result_data

	def test_noupdate_read(self):
		data = DBprocess.read(self.tid)
		result = json.loads(data)
		assert result == noupdate_result_data

	def test_update(self):
		tid = newdata['id']
		indata = json.dumps(newdata)
		DBprocess.update(indata)
		data = DBprocess.read(tid)
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
			newdata1 = json.dumps(newdata1)
			assert e == "'__task__' not found or is False for JSON: {}".format(newdata1)
		try:
			check_json(indata2)
		except Exception as e:
			e = str(e)
			newdata2 = json.dumps(newdata2)
			assert e == "Required key: {key} is not found in JSON string: {s}".format(key='desc', s=newdata2)
		try:
			check_json(indata3)
		except Exception as e:
			e = str(e)
			assert e == "Wrong type for key: {k} with value: {v}".format(k='worker', v=newdata3['worker'])
