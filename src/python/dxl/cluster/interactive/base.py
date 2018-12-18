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
# from jfs.path import Path
# from dxl.cluster.time.timestamps import TaskStamp
from dxl.cluster.time.utils import strf, strp, now
# from ..database.base import check_json
from typing import Dict, Iterable
from ..database.model import TaskState, TaskStateField
import marshmallow as ma


class Schema(ma.Schema):
    id = ma.fields.Integer(allow_none=True)
    scheduler = ma.fields.String(allow_none=True)
    state = TaskStateField(attribute="state")
    create = ma.fields.DateTime(allow_none=True)
    submit = ma.fields.DateTime(allow_none=True)
    finish = ma.fields.DateTime(allow_none=True)
    depends = ma.fields.List(ma.fields.Integer())
    # details = ma.fields.Dict(allow_none=True)


schema = Schema()


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
    def __init__(self, id=None, nb_nodes=None, node_list=None, nb_GPU=None, args=None):
        self.id = id
        if isinstance(self.id, str):
            self.id = int(self.id)
        if nb_nodes is None:
            self.nb_nodes = 0
        else:
            self.nb_nodes = int(nb_nodes)
        self.node_list = node_list
        if nb_GPU is None:
            self.nb_GPU = 0
        else:
            self.nb_GPU = nb_GPU
        if args is None:
            self.args = ''
        else:
            self.args = args

    @classmethod
    def parse_dict(cls, dct: dict):
        return TaskInfo(nb_nodes=dct['nodes'],
                        node_list=dct['node_list'],
                        id=dct['job_id'],
                        nb_GPU=dct['GPUs'],
                        args=dct['args'])

    def to_dict(self) -> Dict[str, str]:
        return {
            'job_id': self.id,
            'nodes': self.nb_nodes,
            'node_list': self.node_list,
            'GPUs': self.nb_GPU,
            'args': self.args
        }

    def update_node_list(self, node_list):
        return TaskInfo(id=self.id,
                        nb_nodes=self.nb_nodes,
                        node_list=node_list,
                        nb_GPU=self.nb_GPU,
                        args=self.args)

    def update_args(self, args):
        return TaskInfo(id=self.id,
                        nb_nodes=self.nb_nodes,
                        node_list=self.node_list,
                        nb_GPU=self.nb_GPU,
                        args=args)



class Task:
    # json_tag = '__task__'

    def __init__(self,
                 id=None,
                 state=None,
                 depends=[],
                 create=None,
                 submit=None,
                 finish=None,
                 scheduler=None,
                 details={}):

        self.id = id

        self.depends = depends

        if state is None:
            state = TaskState.Created
        self.state = state

        self.create = create
        self.submit = submit
        self.finish = finish

        self.scheduler=scheduler
        self.details = details
        self.details["is_user_task"] = details.get("is_user_task", False)

    @property
    def is_created(self):
        return self.state == TaskState.Created

    @property
    def is_pending(self):
        return self.state == TaskState.Pending

    @property
    def is_running(self):
        return self.state == TaskState.Running

    @property
    def is_completed(self):
        return self.state == TaskState.Completed

    @property
    def is_fail(self):
        return self.state == TaskState.Failed

    @property
    def is_user_task(self):
        return self.details["is_user_task"] is True

    def serialization(self):
        return schema.dump(self)

    @classmethod
    def deserialization(cls, s: 'json string'):
        return schema.load(s)

        # pass
    # @property
    # def is_depen_gpu(self):
    #     if self.info != {}:
    #         return self.info['GPUs'] != 0

    def command(self, generate_func=None) -> str:
        if generate_func is None:
            pass

    def to_json(self):
        return json.dumps(self.serialization())

    @classmethod
    def from_json(cls, s):
        return json.loads(s)

    @classmethod
    def from_dict(cls, d):
        return

    def replace_depends(self, sid1, sid2):
        sids = self.depends
        for i in range(0, len(sids)):
            if sids[i] == sid1:
                sids[i] = sid2
        self.depends = sids

    def update_state(self, new_statue):
        return Task(id=self.id,
                    state=new_statue,
                    depends=self.depends,
                    create=self.create,
                    submit=self.submit,
                    finish=self.finish,
                    details=self.details)

    def update_start(self):
        self.create = now()
        return Task(id=self.id,
                    state=self.state,
                    depends=self.depends,
                    create=self.create,
                    submit=self.submit,
                    finish=self.finish,
                    details=self.details)

    def updata_complete(self):
        self.finish = now()
        return Task(id=self.id,
                    state=self.state,
                    depends=self.depends,
                    create=self.create,
                    submit=self.submit,
                    finish=self.finish,
                    details=self.details)

    # @classmethod
    # def from_json(cls, s):
    #     # check_json(s)
    #     return json.loads(s, object_hook=cls.deserialization)
    #
    # @classmethod
    # def serialization(cls, obj):
    #     if isinstance(obj, Task):
    #         return {'id': obj.id,
    #                 'state': obj.state.value,
    #                 'depends': obj.depends,
    #                 "create": obj.create,
    #                 "submit": obj.submit,
    #                 "finish": obj.finish,
    #                 "details": obj.details}
    #     raise TypeError(repr(obj) + " is not JSON serializable")
    #
    # @classmethod
    # def deserialization(cls, dct):
    #     # if cls.json_tag in dct:
    #     return Task(id=dct['id'],
    #                 state=TaskState(dct['state']),
    #                 depends=dct['depends'],
    #                 create=dct['create'],
    #                 submit=dct['submit'],
    #                 finish=dct['finish'])
    #                 # details=
    #                 # )
    #     # return dct
    #
    def __repr__(self):
        return f"Task {self.to_json()}"

    # def __str__(self):
    #     dct = self.serialization(self)
    #     return json.dumps(dct, separators=(',', ':'), indent=4)

    def __hash__(self):
        return id(self)