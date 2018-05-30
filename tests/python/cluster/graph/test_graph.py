from dxl.cluster.interactive.run import TaskSleep
from dxl.cluster.interactive.base import State,Worker,Type
from dxl.cluster.taskgraph.base import Graph
import unittest
from dxpy.time.utils import now
from dxl.cluster.interactive import web,fortest


class TestGraph(unittest.TestCase):
    def test_denepndency(self):
        t1 = TaskSleep(tid=1,desc='sleep 10 s',workdir='.',worker=Worker.Local,ttype=Type.Regular,
                 	state=State.BeforeSubmit,time_stamp=now,dependency=None,is_root=True,data=None)
        t2 = TaskSleep(tid=2,desc='sleep 10 s',workdir='.',worker=Worker.Local,ttype=Type.Regular,
                 	state=State.BeforeSubmit,time_stamp=now,dependency=[t1],is_root=True,data=None)
        g = Graph([t1.id, t2.id], [None, t1.id])
        t = g.all_runable()
        assert t == [1]

    def test_mark(self):
        t1 = fortest.Request().read(1)
        t2 = fortest.Request().read(2)
        g = Graph([t1.id, t2.id], [None, None])
        assert list(g.nodes()) == [1,2]
        g.mark_complete()
        assert list(g.nodes()) == [2]

    


