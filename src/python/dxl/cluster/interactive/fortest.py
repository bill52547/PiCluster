from .run import TaskSleep
from . import base
from .base import Task,State
from dxl.cluster.time.timestamps import TaskStamp
from dxl.cluster.time.utils import strp



def parse_json(s: 'json string'):
    return Task.from_json(s)

class Request(Task):
    def read(self,tid):
        if tid == 1:
            return Task(tid=1, desc='test', workdir='/tmp/test',
                      worker=base.Worker.MultiThreading,
                      ttype=base.Type.Regular,
                      state=base.State.Complete,
                      dependency=None,
                      father=None,
                      time_stamp=TaskStamp(create=strp(
                          "2017-09-22 12:57:44.036185")),
                      data={'sample': 42},
                      is_root=True)
        if tid ==2:
            return Task(tid=2, desc='test', workdir='/tmp/test',
                      worker=base.Worker.MultiThreading,
                      ttype=base.Type.Regular,
                      state=base.State.Pending,
                      dependency=[1],
                      father=None,
                      time_stamp=TaskStamp(create=strp(
                          "2017-09-22 12:57:44.036185")),
                      data={'sample': 42},
                      is_root=True)


def submit(task): 
    task = Task.from_json(task.to_json())
    task.state = State.Pending
    return task


def start(task):
    task = Task.from_json(task.to_json())
    task.start = now()
    task.state = State.Runing
    return task


def complete(task):
    task = Task.from_json(task.to_json())
    task.end = now()
    task.state = State.Complete
    return task
