from dxl.cluster.database import base
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
import json
import unittest

class TestDatabase(unittest.TestCase):
    def setUp(self):
        c['path'] = ':memory:'
        Database.create()
        with open('/home/twj2417/tasksystem/data.json') as fin:
            data = json.load(fin)
        indata = json.dumps(data)
        self.tid = base.DBprocess.create(indata)


    def tearDown(self):
        Database.clear()
        c.back_to_default()


    def test_delete_and_readall(self):
        base.DBprocess.delete(self.tid)
        task_jsons = []
        base.DBprocess.read_all().subscribe(lambda t: task_jsons.append(t))
        newresult = json.dumps(task_jsons)
        assert newresult == '[]'   

    def test_read_and_update(self):
        with open('/home/twj2417/tasksystem/newdata.json','r') as fin:
            inputs = json.load(fin)
        tid = inputs['id']
        indata = json.dumps(inputs)
        base.DBprocess.update(indata)
        data = base.DBprocess.read(tid)
        result1 = json.loads(data) 
        assert result1 == {'__task__': True, 'id': 1, 'desc': 'a new recon task', 'data': {'filename': 'new.h5'}, 'worker': '1', 'type': 'float', 'workdir': '/home/twj2417/Destop', 'dependency': ['task1', 'task2'], 'father': [1],'time_stamp': {'create': '2018-05-24 11:55:41.600000', 'start': '2018-05-24 11:56:12.300000', 'end': '2018-05-26 11:59:23.600000'}, 'state': 'submit', 'is_root': False}
        base.DBprocess.delete(tid)

