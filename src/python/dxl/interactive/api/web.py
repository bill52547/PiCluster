from functools import wraps

import json

from flask import Flask, Response, make_response, request
from flask_restful import Api, Resource, reqparse

from ..exceptions import TaskNotFoundError
from .. import service
from ..config import config as c
from dxpy.web.urls import api_path


class TaskResource(Resource):
    def get(self, id):
        try:
            return Response(
                service.read_py(id), 200, mimetype="application/json")
        except TaskNotFoundError as e:
            return str(e), 404

    def put(self, id):
        try:
            task = request.form['task']
            return Response(
                service.update_py(task), 201, mimetype="application/json")
        except TaskNotFoundError as e:
            return str(e), 404

    def delete(self, id):
        try:
            return Response(
                service.delete_py(id), 200, mimetype="application/json")
        except TaskNotFoundError as e:
            return str(e), 404


class TasksResource(Resource):    
    def get(self):
        task_jsons = []
        service.read_all_py().subscribe(lambda t: task_jsons.append(t))
        return Response(
            self.json.dumps(task_jsons), 200, mimetype="application/json")

    def post(self):
        task = request.form['task']
        res = service.create_py(task)
        return Response(
            self.json.dumps({
                'id': res
            }), 201, mimetype="application/json")


def add_api(api):   
    print(api_path(c['name'], '<int:id>', c['version'], c['base']))
    api.add_resource(TaskResource,
                     api_path(c['name'], '<int:id>', c['version'], c['base']))
    api.add_resource(TasksResource,
                     api_path(c['names'], None, c['version'], c['base']))
