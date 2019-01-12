from flask import request
from flask_restful import Api, Resource, reqparse
from ..database.model import Mastertask, TaskState
from ..backend import Backends
from ..database.transactions import deserialization, serialization
from ..interactive.transaction import TaskTransactions
from rx.subjects import Subject
from ..dispatcher import methodispatch


API_VERSION = 1
TASK_API_URL = f"/api/v{API_VERSION}/tasks"
# TASK_SLURM_API_URL = f"/api/v{API_VERSION}/taskslurm"
# JOIN_API_URL = f"/api/v{API_VERSION}/jointask"


# def post_stream_maker():
#     task_list = []
#
#     def poster(t):
#         nonlocal task_list
#         task_list.append(t)
#         if isinstance(task_list[-1], Mastertask):
#             task_stream = Subject.from_list(task_list)
#             task_list = []
#             return task_stream
#
#     return poster
#
# ps = post_stream_maker()


class TasksBind:
    """
    Bind to transactions to given TaskTransactions, thus fixed database.
    """
    tasks = None

    @classmethod
    def set(cls, tasks: TaskTransactions):
        if cls.tasks is not None:
            if not tasks is cls.tasks:
                raise ValueError("Tasks already binded to another TaskTransactions, plz clear it before set it to another.")
            return
        cls.tasks = tasks

    @classmethod
    def clear(cls):
        cls.tasks = None


class TasksResource(Resource):
    def post(self):
        tpr = TaskParser(request=request)
        return tpr.post()


class TaskParser:
    def __init__(self, request):
        self.request = request

    @property
    def body(self):
        tmp = self.request.json
        if tmp is None:
            tmp = self.request.form
        if tmp is None:
            raise TypeError("No body found in request.")
        return tmp

    @property
    def task_body(self):
        tmp = {}

        def dict_parser(d):
            for k, v in d.items():
                if not isinstance(v, dict):
                    tmp[k] = v
                else:
                    dict_parser(v)
        dict_parser(self.body)
        return tmp
        # return {k: v for k, v in self.body.items() if k not in ["details"]}

    @property
    def task_details(self):
        return {k: v for k, v in self.task_body.items() if k not in ["is_master_task"]}

    @property
    def backend(self):
        return self.task_body["backend"]

    @property
    def is_master_task(self):
        try:
            return self.task_body["is_master_task"]
        except KeyError:
            print("Error! is_master_task field is requested!")

    @methodispatch
    def post(self):
        raise NotImplemented

    def post(self):
        task = deserialization(self.task_details)
        task = TasksBind.tasks.post(task)

        if self.is_master_task:
            masterTask = TasksBind.tasks.post(Mastertask(task_id=task.id,
                                                         backend=self.backend,
                                                         workdir=task.workdir))
            masterTask = TasksBind.tasks.post(masterTask)
            # print(masterTask)
            return serialization(masterTask), 200

        return serialization(task), 200


def add_resource(api, tasks):
    TasksBind.set(tasks)
    api.add_resource(TasksResource, TASK_API_URL)  # /api/v1/tasks
