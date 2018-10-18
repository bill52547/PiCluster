import flask

from flask import Flask, Response, make_response, request, url_for
from flask_restful import Api, Resource, reqparse
from dxl.cluster.database2 import TaskTransactions
from dxl.cluster.database2 import TaskState, Task
import marshmallow as ma

API_VERSION = 1
API_URL = f"/api/v{API_VERSION}/tasks"


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


# Serialization/deserialization utils
class TaskStateField(ma.fields.Field):

    def _serialize(self, value, attr, obj):
        if value is None:
            return ''
        return value.value

    def _deserialize(self, value, attr, data):
        return TaskState(value)


class TasksSchema(ma.Schema):
    id = ma.fields.Integer(allow_none=True)
    scheduler = ma.fields.Url(allow_none=True)
    state = TaskStateField(attribute="state")
    create = ma.fields.DateTime(allow_none=True)
    submit = ma.fields.DateTime(allow_none=True)
    finish = ma.fields.DateTime(allow_none=True)
    depens = ma.fields.List(ma.fields.Integer())


schema = TasksSchema()


class TasksResource(Resource):

    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('state', type=str, help='State cannot be converted')
        args = parser.parse_args()

        if args['state'] == None:
            try:
                result = TasksBind.tasks.read_all()
                return schema.dump(result, many=True), 200
            except Exception as e:
                return {"error": str(e)}, 404

        elif int(args['state']) in [s.value for s in TaskState]:
            try:
                result = TasksBind.tasks.filter(TaskState(int(args['state'])))
                return schema.dump(result, many=True), 200
            except Exception as e:
                return {"error": str(e)}, 404

        else:
            return {"error": f'state {args["state"]} not defined.'}, 404

    def post(self):
        task = Task(**schema.load(request.json))
        result = TasksBind.tasks.create(task)
        return schema.dump(result), 200


class TaskResource(Resource):

    def get(self, id:int):
        try:
            return schema.dump(TasksBind.tasks.read(id)), 200
        except Exception as e:
            return {"error": str(e)}, 404


class TaskAll(Resource):

    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument('state', type=str, help='State cannot be converted')
        args = parser.parse_args()

        if args['state'] == None:
            try:
                result = TasksBind.tasks.count_all()
                return result, 200 #schema.dump(result), 200
            except Exception as e:
                return {"error": str(e)}, 404

        elif int(args['state']) in [s.value for s in TaskState]:
            try:
                result = TasksBind.tasks.count_filter(TaskState(int(args['state'])))
                return result, 200
            except Exception as e:
                return {"error": str(e)}, 404

        else:
            return {"error": f'state {args["state"]} not defined.'}, 404
# class TaskStatistics(Resource):
#     def get(self):
#         try:
#             result = TasksBind
#



def add_resource(api, tasks):
    TasksBind.set(tasks)
    api.add_resource(TasksResource, API_URL) # /api/v1/tasks
    api.add_resource(TaskResource, API_URL+"/<int:id>") # /api/v1/tasks/<int:id
    api.add_resource(TaskAll, API_URL+"/count")


