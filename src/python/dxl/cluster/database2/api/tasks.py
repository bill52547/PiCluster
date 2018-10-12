import flask

from flask import Flask, Response, make_response, request
from flask_restful import Api, Resource, reqparse
from dxl.cluster.database2 import TaskTransactions
from dxl.cluster.database2 import TaskState
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

class TaskResource(Resource):
    def get(self, id:int):
        try:
            result = TasksBind.tasks.read(id)
            return schema.dump(TasksBind.tasks.read(id)), 200
        except Exception as e:
            return {"error": str(e)}, 404

def add_resource(api, tasks):
    TasksBind.set(tasks)
    api.add_resource(TaskResource, API_URL+"/<int:id>")

