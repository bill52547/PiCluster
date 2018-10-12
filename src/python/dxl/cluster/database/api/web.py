from functools import wraps

import json

from flask import Flask, Response, make_response, request
from flask_restful import Api, Resource, reqparse

from ..exceptions import TaskNotFoundError
from ..base import DBprocess
from ...config import config as c
# from dxpy.api.urls import api_path


class TaskResource(Resource):
    def get(self, id):
        try:
            return Response(
                DBprocess.read(id), 200, mimetype="application/json")
        except TaskNotFoundError as e:
            return str(e), 404

    def delete(self, id):
        try:
            return Response(
                DBprocess.delete(id), 200, mimetype="application/json")
        except TaskNotFoundError as e:
            return str(e), 404

    
class TasksResource(Resource):    
    def get(self):
        task_jsons = []
        DBprocess.read_all().subscribe(lambda t: task_jsons.append(t))
        return Response(
            json.dumps(task_jsons), 200, mimetype="application/json")

    def post(self):
        task = request.form['task']     
        res = DBprocess.create(task)
        return Response(
            json.dumps({
                'id': res
            }), 201, mimetype="application/json")

    def put(self):
        try:
            task = request.form['task']
            return Response(
                DBprocess.update(task), 201, mimetype="application/json")
        except TaskNotFoundError as e:
            return str(e), 404

def api_root(version):
    return "/api/v{version}".format(version=version)

def api_path(name, suffix=None, version=None, base=None):
    if base is None:
        base = api_root(version)
    else:
        if base.startswith('/'):
            base = base[1:]
        base = "{root}/{base}".format(root=api_root(version), base=base)
    
    if base.endswith('/'):
        base = base[:-1]    
    if suffix is None:
        return "{base}/{name}".format(base=base, name=name)
    else:
        return "{base}/{name}/{suffix}".format(base=base, name=name, suffix=suffix)

def add_api(api):   
    # print(api_path(c['name'], '<int:id>', c['version'], c['base']))
    api.add_resource(TaskResource,
                     api_path(c['name'], '<int:id>', c['version'], c['base']))
    api.add_resource(TasksResource,
                     api_path(c['names'], None, c['version'], c['base']))
