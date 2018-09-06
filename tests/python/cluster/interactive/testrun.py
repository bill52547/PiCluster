from dxl.cluster.interactive.run import TaskSleep 
from dxl.cluster.interactive.base import State,Worker,Type
import unittest
from dxpy.time.utils import now
#from dxl.cluster.interactive import submit, run


class TestSleep(unittest.TestCase):
	def test_sleep(self):
		t = TaskSleep(desc='sleep 10 s',workdir='.',worker=Worker.Local,ttype=Type.Regular,
                 	state=State.BeforeSubmit)
		assert t.id is None
		t_submitted = t.submit()
		assert t_submitted.state == State.Pending
		t_run = t_submitted.run()
		assert t_run.state == State.Complete


