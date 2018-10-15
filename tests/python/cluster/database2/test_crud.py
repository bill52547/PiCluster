from dxl.cluster.database2 import Task, TaskState
import arrow

def test_create():
    t = Task(state=TaskState.Unknown, scheduler="slurm.sjtu.cluster.pitech.com", create=arrow.now(), depens=[3])