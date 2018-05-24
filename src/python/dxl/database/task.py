""" 
A **Representation** of task
Since it is only a representation, it is not mutable, it has only properties.
No action is allowed.

Task fields:
    id,
    desc,
    workdir,
    worker,
    ttype,
    dependency,
    time_stamp,
    state,
    is_root,
    data
"""
import json
from enum import Enum
from dxpy.file_system.path import Path
from dxpy.time.timestamps import TaskStamp
from dxpy.time.utils import strf, strp, now
from dxpy.task.database import check_json


class State(Enum):
    BeforeSubmit = 0
    Pending = 1
    Runing = 2
    Complete = 3


class Worker(Enum):
    NoAction = 0,
    Local = 1,
    MultiThreading = 2,
    MultiProcessing = 3,
    Slurm = 4


class Type(Enum):
    Regular = 0,
    Command = 1,
    Script = 2


class Task:
    json_tag = '__task__'

    def __init__(self,
                 tid=None,
                 desc='',
                 workdir='.',
                 worker=None,
                 ttype=None,
                 state=None,
                 time_stamp=None,
                 dependency=None,
                 is_root=True,
                 data=None):
        self.id = tid
        self.desc = desc
        self.workdir = Path(workdir).abs
        if worker is None:
            worker = Worker.NoAction
        self.worker = worker
        if time_stamp is None:
            time_stamp = TaskStamp.create_now()
        self.time_stamp = time_stamp
        if dependency is None:
            dependency = []
        self.dependency = dependency
        if ttype is None:
            ttype = Type.Regular
        self.type = ttype
        if state is None:
            state = State.BeforeSubmit
        self.state = state
        self.is_root = is_root
        if data is None:
            data = {}
        self.data = data

    @property
    def is_pending(self):
        return self.state == State.Pending

    @property
    def is_before_submit(self):
        return self.state == State.BeforeSubmit

    @property
    def is_running(self):
        return self.state == State.Runing

    @property
    def is_complete(self):
        return self.state == State.Complete

    def command(self, generate_func=None) -> str:
        if generate_func is None:
            pass

    def to_json(self):
        return json.dumps(self.serialization(self))

    @classmethod
    def from_json(cls, s):
        check_json(s)
        return json.loads(s, object_hook=cls.deserialization)

    @classmethod
    def serialization(cls, obj):
        if isinstance(obj, Task):
            return {cls.json_tag: True,
                    'id': obj.id,
                    'desc': obj.desc,
                    'workdir': obj.workdir,
                    'worker': obj.worker.name,
                    'type': obj.type.name,
                    'state': obj.state.name,
                    'dependency': obj.dependency,
                    'time_stamp': {
                        'create': strf(obj.time_stamp.create),
                        'start': strf(obj.time_stamp.start),
                        'end': strf(obj.time_stamp.end)
                    },
                    'is_root': obj.is_root,
                    'data': obj.data, }
        raise TypeError(repr(obj) + " is not JSON serializable")

    @classmethod
    def deserialization(cls, dct):
        if cls.json_tag in dct:
            return Task(tid=dct['id'],
                        desc=dct['desc'],
                        workdir=dct['workdir'],
                        worker=Worker[dct['worker']],
                        ttype=Type[dct['type']],
                        state=State[dct['state']],
                        time_stamp=TaskStamp(
                            create=strp(dct['time_stamp']['create']),
                            start=strp(dct['time_stamp']['start']),
                            end=strp(dct['time_stamp']['end'])),
                        dependency=dct['dependency'],
                        is_root=dct['is_root'],
                        data=dct['data'])
        return dct

    def __str__(self):
        dct = self.serialization(self)
        dct['time_stamp'] = {'create': strf(self.time_stamp.create),
                             'start': strf(self.time_stamp.start),
                             'end': strf(self.time_stamp.end)}
        return json.dumps(dct, separators=(',', ':'), indent=4)

    def __hash__(self):
        return id(self)


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
