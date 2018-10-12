import flask

from flask import Flask, Response, make_response, request
from flask_restful import Api, Resource, reqparse
from dxl.cluster.database2 import TaskTransactions
from dxl.cluster.database2 import TaskState
import marshmallow as ma

class TasksBind:
    tasks = None
    @classmethod
    def set(cls, tasks: TaskTransactions):
        cls.tasks = tasks

    @classmethod
    def clear(cls):
        cls.tasks = None

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



class TaskResource(Resource):
    def get(self, id:int):
        try:
            return TasksBind.tasks.read(id)
        except Exception as e:
            pass
