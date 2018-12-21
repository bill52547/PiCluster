from flask import request
from flask_restful import Api, Resource, reqparse
from ...database import Task, TaskTransactions
from ...database.model import TaskSlurm, TaskSimu, taskSchema, taskSlurmSchema, taskSimuSchema
from ...backend import Backends

API_VERSION = 1
TASK_API_URL = f"/api/v{API_VERSION}/tasks"
TASK_SLURM_API_URL = f"/api/v{API_VERSION}/taskslurm"
JOIN_API_URL = f"/api/v{API_VERSION}/jointask"


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

    # def get(self):
    #     parser = reqparse.RequestParser()
    #     parser.add_argument('state', type=str, help='State cannot be converted')
    #     args = parser.parse_args()
    #
    #     if args['state'] == None:
    #         try:
    #             result = TasksBind.tasks.read_all()
    #             return taskSchema.dump(result, many=True), 200
    #         except Exception as e:
    #             return {"error": str(e)}, 404
    #
    #     elif int(args['state']) in [s.value for s in TaskState]:
    #         try:
    #             result = TasksBind.tasks.filter(TaskState(int(args['state'])))
    #             return taskSchema.dump(result, many=True), 200
    #         except Exception as e:
    #             return {"error": str(e)}, 404
    #
    #     else:
    #         return {"error": f'state {args["state"]} not defined.'}, 404

    def post(self):
        tpr = TaskPoster(request=request)
        return tpr.post()
        # body = request.json
        # if body is None:
        #     body = request.form
        # if body is None:
        #     raise TypeError("No body data found.")

        # taskBody = {k: v for k, v in body.items() if k not in ["details"]}
        # if len(body["details"]):
        #     is_user_task = body["details"]["is_user_task"]
        #     taskSlurmBody = {k:v for k,v in body["details"].items() if k not in ["is_user_task"]}
        # else:
        #     is_user_task = False
        #     taskSlurmBody = {}

        # task = Task(**taskSchema.load(taskBody))
        # result_task = TasksBind.tasks.create(task)
        #
        # taskSlurm = TaskSlurm(**taskSlurmSchema.load(taskSlurmBody))
        # result_taskSlurm = TasksBind.tasks.create_taskSlurm(taskSlurm, result_task.id)
        #
        # if is_user_task:
        #     result_taskSimu = TasksBind.tasks.create_taskSimu(TaskSimu(), result_taskSlurm.id)
        #     return taskSimuSchema.dump(result_taskSimu), 200
        #
        # return taskSlurmSchema.dump(result_taskSlurm), 200


class TaskPoster:
    def __init__(self, request, backend=Backends.Slurm):
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
        return {k:v for k,v in self.body["details"].items() if k not in ["is_user_task"]}

    @property
    def is_user_task(self):
        try:
            return self.body["details"]["is_user_task"]
        except KeyError:
            print("Error! is_user_task field is requested!")

    def post(self):
        if self.backend is Backends.Slurm:
            task = Task(**taskSchema.load(self.task_body))
            result_task = TasksBind.tasks.create(task)

            taskSlurm = TaskSlurm(**taskSlurmSchema.load(self.task_details))
            result_taskSlurm = TasksBind.tasks.create_taskSlurm(taskSlurm, result_task.id)

            if self.is_user_task:
                result_taskSimu = TasksBind.tasks.create_taskSimu(TaskSimu(), result_taskSlurm.id)
                return taskSimuSchema.dump(result_taskSimu), 200

            return taskSlurmSchema.dump(result_taskSlurm), 200

# class TaskResource(Resource):
#
#     def get(self, id:int):
#         try:
#             return taskSchema.dump(TasksBind.tasks.read(id)), 200
#         except Exception as e:
#             return {"error": str(e)}, 404
#
#     def patch(self, id:int):
#         try:
#             body = request.json
#             if body is None:
#                 body = request.form
#             if body is None:
#                 raise TypeError("No body data found.")
#             task_patch = taskSchema.load(body)
#             print(body)
#             print(task_patch)
#             TasksBind.tasks.update(id, {k: v for k, v in task_patch.items() if v is not None})
#             return taskSchema.dump(TasksBind.tasks.read(id)), 200
#         except Exception as e:
#             return {"error": str(e)}, 404


# class TaskAll(Resource):
#
#     def get(self):
#         parser = reqparse.RequestParser()
#         parser.add_argument('state', type=str, help='State cannot be converted')
#         args = parser.parse_args()
#
#         if args['state'] == None:
#             try:
#                 result = TasksBind.tasks.count_all()
#                 return result, 200 #schema.dump(result), 200
#             except Exception as e:
#                 return {"error": str(e)}, 404
#
#         elif int(args['state']) in [s.value for s in TaskState]:
#             try:
#                 result = TasksBind.tasks.count_filter(TaskState(int(args['state'])))
#                 return result, 200
#             except Exception as e:
#                 return {"error": str(e)}, 404
#
#         else:
#             return {"error": f'state {args["state"]} not defined.'}, 404
#
#
# class TaskSlurmResource(Resource):
#     def get(self, id):
#         try:
#             return taskSlurmSchema.dump(TasksBind.tasks.read_taskSlurm(id)), 200
#         except Exception as e:
#             return {"error": str(e)}, 404
#
#     def patch(self, id: int):
#         try:
#             body = request.json
#             if body is None:
#                 body = request.form
#             if body is None:
#                 raise TypeError("No body data found.")
#             task_slurm_patch = taskSlurmSchema.load(body)
#             TasksBind.tasks.task_slurm_update(id, {k: v for k, v in task_slurm_patch.items() if v is not None})
#             return taskSlurmSchema.dump(TasksBind.tasks.read_taskSlurm(id)), 200
#         except Exception as e:
#             return {"error": str(e)}, 404
#
#
# class JoinResource(Resource):
#     def get(self, id):
#         try:
#             return taskSlurmSchema.dump(TasksBind.tasks.read_taskSlurm_by_taskid(id)), 200
#         except Exception as e:
#             return {"error": str(e)}, 404


def add_resource(api, tasks):
    TasksBind.set(tasks)
    api.add_resource(TasksResource, TASK_API_URL) # /api/v1/tasks
    # api.add_resource(TaskResource, TASK_API_URL + "/<int:id>") # /api/v1/tasks/<int:id>
    # api.add_resource(TaskAll, TASK_API_URL + "/count")
    #
    # api.add_resource(TaskSlurmResource, TASK_SLURM_API_URL + "/<int:id>")
    # api.add_resource(JoinResource, JOIN_API_URL + "/<int:id>")