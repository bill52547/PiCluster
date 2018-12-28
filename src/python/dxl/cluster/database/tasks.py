from flask import request
from flask_restful import Api, Resource, reqparse
from ..database.model import Mastertask
from ..backend import Backends
from ..database.transactions import deserialization, serialization
from ..interactive.transaction import TaskTransactions


API_VERSION = 1
TASK_API_URL = f"/api/v{API_VERSION}/tasks"
# TASK_SLURM_API_URL = f"/api/v{API_VERSION}/taskslurm"
# JOIN_API_URL = f"/api/v{API_VERSION}/jointask"


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
        tpr = TaskPoster(request=request)
        return tpr.post()


class TaskPoster:
    def __init__(self, request, backend=Backends.slurm):
        self.backend = backend
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
        return {k: v for k, v in self.body.items() if k not in ["details"]}

    @property
    def task_details(self):
        return {k: v for k, v in self.body["details"].items() if k not in ["is_user_task"]}

    @property
    def is_master_task(self):
        try:
            return self.body["details"]["is_user_task"]
        except KeyError:
            print("Error! is_user_task field is requested!")


    def post(self):
        if self.backend is Backends.slurm:
            task = deserialization(self.task_body)
            task = TasksBind.tasks.post(task)

            slurmTask = deserialization(self.task_details)
            slurmTask.task_id = task.id
            slurmTask = TasksBind.tasks.post(slurmTask, task.id)

            if self.is_master_task:
                masterTask = TasksBind.tasks.post(Mastertask(backend=Backends.slurm.value, id=slurmTask.id))
                return serialization(masterTask), 200

            return serialization(slurmTask), 200


def add_resource(api, tasks):
    TasksBind.set(tasks)
    api.add_resource(TasksResource, TASK_API_URL)  # /api/v1/tasks
