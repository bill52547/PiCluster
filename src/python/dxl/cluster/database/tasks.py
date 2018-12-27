from flask import request
from flask_restful import Api, Resource, reqparse
from ..database import Task #, TaskTransactions
from ..database.model import SlurmTask, Mastertask, taskSchema, slurmTaskSchema, masterTaskschema
from ..backend import Backends
from ..interactive import post
from ..database.transactions import deserialization, serialization
from ..interactive.web import post


# API_VERSION = 1
# TASK_API_URL = f"/api/v{API_VERSION}/tasks"
# TASK_SLURM_API_URL = f"/api/v{API_VERSION}/taskslurm"
# JOIN_API_URL = f"/api/v{API_VERSION}/jointask"


# class TasksBind:
#     """
#     Bind to transactions to given TaskTransactions, thus fixed database.
#     """
#     tasks = None
#     @classmethod
#     def set(cls, tasks: TaskTransactions):
#         if cls.tasks is not None:
#             if not tasks is cls.tasks:
#                 raise ValueError("Tasks already binded to another TaskTransactions, plz clear it before set it to another.")
#             return
#         cls.tasks = tasks
#
#     @classmethod
#     def clear(cls):
#         cls.tasks = None


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
    def is_user_task(self):
        try:
            return self.body["details"]["is_user_task"]
        except KeyError:
            print("Error! is_user_task field is requested!")

    def _on_independent(self, depends):
        for d in depends:
            pass
            # Request.task

    def post(self):
        if self.backend is Backends.slurm:
            task = deserialization(self.task_body)
            task = post(task)

            slurmTask = deserialization(self.task_details)
            slurmTask.task_id = task.id
            slurmTask = post(slurmTask)

            if self.is_user_task:
                masterTask = post(Mastertask(backend=Backends.slurm.value, backend_task_id=slurmTask.id))
                return serialization(masterTask), 200

            return serialization(slurmTask), 200

            # task = Task(**taskSchema.load(self.task_body))
            # result_task = TasksBind.tasks.create(task)
            #
            # slurmTask = SlurmTask(**slurmTaskSchema.load(self.task_details))
            # result_slurmTask = TasksBind.tasks.create_taskSimu(slurmTask, result_task.id)
            #
            # if self.is_user_task:
            #     result_taskSimu = TasksBind.tasks.create_taskSimu(Mastertask(), result_slurmTask.id)
            #     return masterTaskschema.dump(result_taskSimu), 200
            #
            # return slurmTaskSchema.dump(result_slurmTask), 200


def add_resource(api):
    api.add_resource(TasksResource, TASK_API_URL)  # /api/v1/tasks
