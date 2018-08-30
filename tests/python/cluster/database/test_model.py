import unittest
import pytest
import sqlalchemy
from dxpy.time.utils import strp
from dxl.cluster.config import config
from dxl.cluster.database.base import DBprocess
from dxl.cluster.database.model import *
from dxl.cluster.config import config as c

config_path_sqllite = config.path_sqllite

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


class TestDatabase(unittest.TestCase):

	def tearDown(self):
		Database.clear()
		Database.engine = None
		DBprocess.clear_session()

	def test_engine(self):
		assert Database.engine is None

	def test_create_engine(self):
		Database.create_engine()
		assert isinstance(Database.engine, sqlalchemy.engine.base.Engine)
		assert repr(Database.engine) == "Engine(%s)" % config_path_sqllite

	def test_get_or_create_engine(self):
		Database.create_engine()
		assert repr(Database.get_or_create_engine()) == "Engine(%s)" % config_path_sqllite

	def test_get_or_create_engine2(self):
		assert repr(Database.get_or_create_engine()) == "Engine(%s)" % config_path_sqllite

	def test_session_maker(self):
		result = Database.session_maker()
		assert isinstance(result, sqlalchemy.orm.session.sessionmaker)
		assert repr(
			result) == "sessionmaker(class_='Session', bind=Engine(%s), autoflush=True, autocommit=False, expire_on_commit=True)" % config_path_sqllite

	def test_create(self):
		Database.create()
		assert Database.engine is not None
		assert isinstance(Database.engine, sqlalchemy.engine.base.Engine)
		assert repr(Database.engine) == "Engine(%s)" % config_path_sqllite

	def test_session(self):
		result = Database.session()
		assert isinstance(result, sqlalchemy.orm.session.Session)

	def test_clear(self):
		c['path'] = ':memory:'
		Database.create()
		indata = json.dumps(data)
		DBprocess.create_session()
		tid = DBprocess.create(indata)
		result = DBprocess.get_or_create_session().query(TaskDB).get(tid)
		assert str(result) == '<Task %s>' % tid
		del result
		Database.clear()
		result2 = DBprocess.get_or_create_session().query(TaskDB).get(tid)
		assert result2 is None
		c.back_to_default()
		DBprocess.clear_session()

	def test_TaskDB(self):
		task_db = TaskDB(desc=data['desc'],
						 data=json.dumps(data['data']),
						 state=data['state'],
						 workdir=data['workdir'],
						 worker=data['worker'],
						 ttype=data['type'],
						 dependency=json.dumps(data['dependency']),
						 father=json.dumps(data['father']),
						 time_create=strp(data['time_stamp']['create']),
						 is_root=data['is_root'],
						 script_file=json.dumps(data['script_file']),
						 info=json.dumps(data['info']))
		assert task_db.desc == data['desc']
		assert task_db.father == str(data['father'])
