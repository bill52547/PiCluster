import unittest
from dxl.cluster.backend import forcluster
from dxl.cluster.backend import backend


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

    def 