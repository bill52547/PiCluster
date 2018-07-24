import unittest
import json
from dxl.cluster.interactive import base
from dxpy.time.timestamps import TaskStamp
from dxpy.time.utils import strp


class Testtask(unittest.TestCase):
    def test_to_json(self):
        t = base.Task(tid=10, desc='test', workdir='/tmp/test',
                      worker=base.Worker.MultiThreading,
                      ttype=base.Type.Regular,
                      dependency=[1, 2, 3],
                      father=[1],
                      time_stamp=TaskStamp(create=strp(
                          "2017-09-22 12:57:44.036185")),
                      data={'sample': 42},
                      is_root=True)
        s = t.to_json()
        dct = json.loads(s)
        self.assertEqual(dct['id'], 10)
        self.assertEqual(dct['father'],[1])
        self.assertEqual(dct['desc'], 'test')
        self.assertEqual(dct['dependency'], [1, 2, 3])
        self.assertEqual(dct['data'], {'sample': 42})
        self.assertEqual(dct['type'], 'Regular')
        self.assertEqual(dct['workdir'], '/tmp/test')
        self.assertEqual(dct['worker'], 'MultiThreading')
        self.assertEqual(dct['is_root'], True)
        self.assertEqual(dct['time_stamp'], {
                         'create': "2017-09-22 12:57:44.036185", 'start': None, 'end': None})
        self.assertEqual(dct['state'], 'BeforeSubmit')

    def test_from_json(self):
        dct = {
            '__task__': True,
            'id': 10,
            'desc': 'test',
            'workdir': '/tmp/test',
            'worker': 'Slurm',
            'type': 'Script',
            'dependency': [1, 2, 3],
            'father':[1],
            'data': {'sample': 42},
            'is_root': True,
            'time_stamp': {
                'create': "2017-09-22 12:57:44.036185",
                'start': None,
                'end': None
            },
            'state': 'BeforeSubmit'
        }
        t = base.Task.from_json(json.dumps(dct))
        self.assertEqual(t.id, 10)
        self.assertEqual(t.desc, 'test')
        self.assertEqual(t.workdir, '/tmp/test')
        self.assertEqual(t.worker, base.Worker.Slurm)
        self.assertEqual(t.type, base.Type.Script)
        self.assertEqual(t.dependency, [1, 2, 3])
        self.assertEqual(t.father,[1])
        self.assertEqual(t.data, {'sample': 42})
        self.assertEqual(t.is_root, True)
        self.assertEqual(t.time_stamp.create, strp(
            "2017-09-22 12:57:44.036185"))
        self.assertEqual(t.state, base.State.BeforeSubmit)

    def test_replce_dependency(self):
        dct = {
            '__task__': True,
            'id': 10,
            'desc': 'test',
            'workdir': '/tmp/test',
            'worker': 'Slurm',
            'type': 'Script',
            'dependency': [1, 2, 3],
            'father':[1],
            'data': {'sample': 42},
            'is_root': True,
            'time_stamp': {
                'create': "2017-09-22 12:57:44.036185",
                'start': None,
                'end': None
            },
            'state': 'BeforeSubmit'
        }
        t = base.Task.from_json(json.dumps(dct))
        t.replace_dependency(2,4)
        assert t.dependency == [1,4,3]

    def test_update_state(self):
        dct = {
            '__task__': True,
            'id': 10,
            'desc': 'test',
            'workdir': '/tmp/test',
            'worker': 'Slurm',
            'type': 'Script',
            'dependency': [1, 2, 3],
            'father':[1],
            'data': {'sample': 42},
            'is_root': True,
            'time_stamp': {
                'create': "2017-09-22 12:57:44.036185",
                'start': None,
                'end': None
            },
            'state': 'BeforeSubmit'
        }
        t = base.Task.from_json(json.dumps(dct))
        t.update_state(base.State.Pending)
        assert t.state == base.State.Pending