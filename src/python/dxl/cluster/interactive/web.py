import json
from functools import wraps
import requests
import rx

from .exceptions import TaskDatabaseConnectionError, TaskNotFoundError
from ..config import config as c
from dxpy.web.urls import req_url
from dxpy.time.utils import now
from .base import Task,State


def connection_error_handle(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.ConnectionError as e:
            raise TaskDatabaseConnectionError(
                "Task database server connection failed. Details:\n{e}".format(e=e))
    return wrapper

def url(tid=None):    
    if tid is None:
        return req_url(c['names'], 'localhost', c['port'], None, c['version'], c['base'])
    else:
        return req_url(c['name'], 'localhost', c['port'], tid, c['version'], c['base'])

def parse_json(s: 'json string'):
    return Task.from_json(s)

    
class Request(Task):                 
    @connection_error_handle
    def create(self,task):
        task_json = task.to_json()
        r = requests.post(url(), {'task': task_json}).json()
        task.id = r['id']
        return task

    @connection_error_handle
    def read(self,tid):
        r = requests.get(url(tid))
        if r.status_code == 200:
            return parse_json(r.text)  
        else:
            raise TaskNotFoundError(tid)

    @connection_error_handle
    def read_all(self):
        tasks = requests.get(url()).text
        task = json.loads(tasks)
        return (rx.Observable.from_(task).map(parse_json))

    @connection_error_handle
    def update(self,task):
        task_json = task.to_json()
        r = requests.put(url(), {'task': task_json})
        if r.status_code == 404:
            raise TaskNotFoundError(task_json['id'])

    @connection_error_handle
    def delete(self,tid):
        r = requests.delete(url(tid))
        if r.status_code == 404:
            raise TaskNotFoundError(tid)


def submit(task): 
    task = Task.from_json(task.to_json())
    task.state = State.Pending
    Request().update(task)
    return task


def start(task):
    task = Task.from_json(task.to_json())
    task.start = now()
    task.state = State.Runing
    Request().update(task)
    return task


def complete(task):
    task = Task.from_json(task.to_json())
    task.end = now()
    task.state = State.Complete
    Request().update(task)
    return task