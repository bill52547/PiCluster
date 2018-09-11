import rx
from dxpy.time.timestamps import TaskStamp

from dxl.cluster.database.base import DBprocess
from dxl.cluster.interactive.run import TaskSleep
from dxl.cluster.interactive.base import State, Worker, Type, Task
from dxl.cluster.taskgraph.base import Graph
import unittest
from dxpy.time.utils import now, strp
from dxl.cluster.interactive import web, fortest
from dxl.cluster.config import config as c
from dxl.cluster.database.model import Database
from dxl.cluster.interactive import base
from dxl.cluster.taskgraph.depens import *

t1 = TaskSleep(tid=3, desc='sleep 10 s', workdir='.', worker=Worker.Local, ttype=Type.Regular,
               state=State.BeforeSubmit, time_stamp=now, dependency=None, is_root=True, data=None)
t2 = TaskSleep(tid=4, desc='sleep 10 s', workdir='.', worker=Worker.Local, ttype=Type.Regular,
               state=State.BeforeSubmit, time_stamp=now, dependency=[t1], is_root=True, data=None)

task1 = Task(tid=1, desc='test', workdir='/tmp/test',
             worker=base.Worker.MultiThreading,
             ttype=base.Type.Regular,
             state=base.State.Complete,
             dependency=None,
             father=None,
             time_stamp=TaskStamp(create=strp(
                 "2017-09-22 12:57:44.036185")),
             data={'sample': 42},
             is_root=True)

task2 = Task(tid=2, desc='test', workdir='/tmp/test',
             worker=base.Worker.MultiThreading,
             ttype=base.Type.Regular,
             state=base.State.Pending,
             dependency=[1],
             father=None,
             time_stamp=TaskStamp(create=strp(
                 "2017-09-22 12:57:44.036185")),
             data={'sample': 42},
             is_root=True)


def test_mark():
    Database.clear()
    c['path'] = ':memory:'
    Database.create()

    tt1 = web.Request().create(task1)
    tt2 = web.Request().create(task2)
    g = Graph([tt1.id, tt2.id], [tt1.dependency, tt2.dependency])
    if len(list(g.nodes())) == 3:
        if list(g.nodes())[2] == 1:
            assert list(g.nodes()) == [tt1.id, tt2.id, 1]
    else:
        assert list(g.nodes()) == [tt1.id, tt2.id]
    g.mark_complete()
    assert list(g.nodes()) == [tt2.id]

    DBprocess.clear_session()
    Database.clear()
    c.back_to_default()


def test_all_runable():
    g = Graph([t1.id, t2.id], [None, t1.id])
    t = g.all_runable()
    assert t == [3]
