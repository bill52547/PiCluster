from dxl.cluster.taskgraph import service
from dxl.cluster.interactive import web
from dxl.cluster.interactive import base
from dxl.cluster.interactive.run import TaskSleep
from dxpy.time.timestamps import TaskStamp
from dxpy.time.utils import strp
from dxpy.time.utils import now
import unittest
import rx

task1 = base.Task(desc='test', workdir='/tmp/test',
                      worker=base.Worker.MultiThreading,
                      ttype=base.Type.Regular,
                      state=base.State.Pending,
                      dependency=None,
                      time_stamp=TaskStamp(create=strp(
                          "2017-09-22 12:57:44.036185")),
                      data={'sample': 42},
                      is_root=True)



class TestDemon(unittest.TestCase):   
    def test_cycle(self):
        service.DemonService().cycle()
        r = web.Request().read(2)
        assert r.state==base.State.Pending
        
        