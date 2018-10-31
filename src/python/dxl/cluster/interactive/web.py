import json
from functools import wraps
import requests
import rx

from .exceptions import TaskDatabaseConnectionError, TaskNotFoundError
from ..config import config
from .base import Task
from ..database2.model import TaskState

import datetime


def now(local=False):
    if local:
        return datetime.datetime.now()
    else:
        return datetime.datetime.utcnow()


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


def req_url(name, ip=None, port=None, suffix=None, version=None, base=None):
    return 'http://{ip}:{port}{path}'.format(ip=ip, port=port, path=api_path(name, suffix, version, base))


def connection_error_handle(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.ConnectionError as e:
            raise TaskDatabaseConnectionError(
                "Task database server connection failed. Details:\n{e}".format(e=e))
    return wrapper


def url(id=None):
    """
    Url builder, works with helper functions api_root, api_path and req_url.

    :param id: Id of a task to be used to build a request url.
    :return: Request url.
    """

    if id is None:
        return req_url(config['name'], 'localhost', config['port'], None, config['version'], config['base'])
    else:
        return req_url(config['name'], 'localhost', config['port'], id, config['version'], config['base'])


def parse_json(s: 'json string'):
    return Task.from_json(s)

    
class Request:

    @classmethod
    @connection_error_handle
    def create(cls, task):
        task_json = task.to_json()
        # r = requests.post(url(), {'task': task_json}).json()
        r = requests.post(url(), task_json, headers={'Content-Type': "application/json"}).json()
        task.id = r['id']
        return task

    @classmethod
    @connection_error_handle
    def read(cls, id):
        r = requests.get(url(id))
        if r.status_code == 200:
            return parse_json(r.text)  
        else:
            raise TaskNotFoundError(id)

    @classmethod
    @connection_error_handle
    def read_all(cls):
        tasks = requests.get(url()).text
        task = json.loads(tasks)
        return rx.Observable.from_(task).map(parse_json)

    @classmethod
    @connection_error_handle
    def update(cls, task):
        task_json = task.to_json()
        r = requests.put(url(), {'task': task_json})
        if r.status_code == 404:
            raise TaskNotFoundError(task_json['id'])

    @classmethod
    @connection_error_handle
    def delete(cls, id):
        r = requests.delete(url(id))
        if r.status_code == 404:
            raise TaskNotFoundError(id)


def submit(task): 
    task = Task.from_json(task.to_json())
    task.state = TaskState.Pending
    Request().update(task)
    return task


def start(task):
    task = Task.from_json(task.to_json())
    task.start = now()
    task.state = TaskState.Runing
    Request().update(task)
    return task


def complete(task):
    task = Task.from_json(task.to_json())
    task.end = now()
    task.state = TaskState.Complete
    Request().update(task)
    return task