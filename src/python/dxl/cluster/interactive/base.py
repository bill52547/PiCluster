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
# from dxpy.file_system.path import Path
from jfs.path import Path
from dxl.cluster.time.timestamps import TaskStamp
from dxl.cluster.time.utils import strf, strp, now
from ..database.base import check_json
from typing import Dict, Iterable


class State(Enum):
    BeforeSubmit = 0
    Pending = 1
    Runing = 2
    Complete = 3
    Failed = 4


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


class TaskInfo:
    def __init__(self, sid=None, nb_nodes=None, node_list=None, nb_GPU=None, args=None):
        self.sid = sid
        if isinstance(self.sid, str):
            self.sid = int(self.sid)
        if nb_nodes == None:
            self.nb_nodes = 0
        else:
            self.nb_nodes = int(nb_nodes)
        self.node_list = node_list
        if nb_GPU == None:
            self.nb_GPU = 0
        else:
            self.nb_GPU = nb_GPU
        if args == None:
            self.args = ''
        else:
            self.args = args

    @classmethod
    def parse_dict(cls, dct: dict):
        return TaskInfo(nb_nodes=dct['nodes'],
                        node_list=dct['node_list'],
                        sid=dct['job_id'],
                        nb_GPU=dct['GPUs'],
                        args=dct['args'])

    def to_dict(self) -> Dict[str, str]:
        return {
            'job_id': self.sid,
            'nodes': self.nb_nodes,
            'node_list': self.node_list,
            'GPUs': self.nb_GPU,
            'args': self.args
        }

    def update_node_list(self, node_list):
        return TaskInfo(sid=self.sid,
                        nb_nodes=self.nb_nodes,
                        node_list=node_list,
                        nb_GPU=self.nb_GPU,
                        args=self.args
                        )

    def update_args(self, args):
        return TaskInfo(sid=self.sid,
                        nb_nodes=self.nb_nodes,
                        node_list=self.node_list,
                        nb_GPU=self.nb_GPU,
                        args=args
                        )


class Task:
    json_tag = '__task__'

    def __init__(self,
                 tid=None,
                 desc='',
                 workdir='.',
                 worker=None,
                 father=None,
                 ttype=None,
                 state=None,
                 time_stamp=None,
                 dependency=None,
                 is_root=True,
                 data=None,
                 script_file=None,
                 info=None):
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
        if father is None:
            father = []
        self.father = father
        if script_file is None:
            script_file = []
        self.script_file = script_file
        if info is None:
            info = TaskInfo().to_dict()
        self.info = info

    # def __eq__(self, m):
    #     return isinstance(m, Task) and m.unbox() == self.unbox()
    #
    # def unbox(self):
    #
    #     return [self.id, self.desc, self.workdir, self.worker, self.father, self.type, self.state, self.time_stamp,
    #             self.dependency, self.is_root, self.data, self.script_file, self.info]
    # return self.time_stamp
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

    @property
    def is_fail(self):
        return self.state == State.Failed

    @property
    def is_depen_gpu(self):
        if self.info != {}:
            return self.info['GPUs'] != 0

    def command(self, generate_func=None) -> str:
        if generate_func is None:
            pass

    def to_json(self):
        return json.dumps(self.serialization(self))

    def replace_dependency(self, sid1, sid2):
        sids = self.dependency
        for i in range(0, len(sids)):
            if sids[i] == sid1:
                sids[i] = sid2
        self.dependency = sids

    def update_state(self, new_statue):
        return Task(tid=self.id,
                    desc=self.desc,
                    workdir=self.workdir,
                    worker=self.worker,
                    time_stamp=self.time_stamp,
                    dependency=self.dependency,
                    ttype=self.type,
                    state=new_statue,
                    is_root=self.is_root,
                    data=self.data,
                    father=self.father,
                    script_file=self.script_file,
                    info=self.info)

    def update_info(self, new_info):
        return Task(tid=self.id,
                    desc=self.desc,
                    workdir=self.workdir,
                    worker=self.worker,
                    time_stamp=self.time_stamp,
                    dependency=self.dependency,
                    ttype=self.type,
                    state=self.state,
                    is_root=self.is_root,
                    data=self.data,
                    father=self.father,
                    script_file=self.script_file,
                    info=new_info)

    def update_start(self):
        self.time_stamp.start = now()
        return Task(tid=self.id,
                    desc=self.desc,
                    workdir=self.workdir,
                    worker=self.worker,
                    time_stamp=self.time_stamp,
                    dependency=self.dependency,
                    ttype=self.type,
                    state=self.state,
                    is_root=self.is_root,
                    data=self.data,
                    father=self.father,
                    script_file=self.script_file,
                    info=self.info)

    def updata_complete(self):
        self.time_stamp.end = now()
        return Task(tid=self.id,
                    desc=self.desc,
                    workdir=self.workdir,
                    worker=self.worker,
                    time_stamp=self.time_stamp,
                    dependency=self.dependency,
                    ttype=self.type,
                    state=self.state,
                    is_root=self.is_root,
                    data=self.data,
                    father=self.father,
                    script_file=self.script_file,
                    info=self.info)

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
                    'father': obj.father,
                    'time_stamp': {
                        'create': str(obj.time_stamp.create),
                        'start': str(obj.time_stamp.start),
                        'end': str(obj.time_stamp.end)
                    },
                    'is_root': obj.is_root,
                    'data': obj.data,
                    'script_file': obj.script_file,
                    'info': obj.info}
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
                        father=dct['father'],
                        time_stamp=TaskStamp(
                            create=strp(dct['time_stamp']['create']),
                            start=strp(dct['time_stamp']['start']),
                            end=strp(dct['time_stamp']['end'])),
                        dependency=dct['dependency'],
                        is_root=dct['is_root'],
                        data=dct['data'],
                        script_file=dct['script_file'],
                        info=dct['info'])
        return dct

    def __str__(self):
        dct = self.serialization(self)
        dct['time_stamp'] = {'create': str(self.time_stamp.create),
                             'start': str(self.time_stamp.start),
                             'end': str(self.time_stamp.end)}
        return json.dumps(dct, separators=(',', ':'), indent=4)

    def __hash__(self):
        return id(self)
