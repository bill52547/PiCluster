import json
from functools import wraps
import requests
import rx

from .exceptions import TaskDatabaseConnectionError, TaskNotFoundError
from ..config import config
from .base import Task
from ..database2.model import TaskState
from typing import List
from ..database2.model import taskSchema
import json

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

def url_taskSlurm(id=None):
    if id is None:
        return req_url(ip="localhost",
                       port=config['port'],
                       version=config['version'],
                       name="taskslurm",
                       suffix=None)
    else:
        return req_url(ip="localhost",
                       port=config['port'],
                       version=config['version'],
                       name="taskslurm",
                       suffix=id)

def parse_json(s: 'json string'):
    return Task.from_json(s)

    
class Request:

    @classmethod
    @connection_error_handle
    def create(cls, task):
        task_json = task.to_json()
        # r = requests.post(url(), {'task': task_json}).json()
        # print("************request************")
        # print(task_json)
        r = requests.post(url(), json=task_json).json()
        task.id = r['id']
        return task

    @classmethod
    @connection_error_handle
    def read(cls, id: int):
        r = requests.get(url(id))
        if r.status_code == 200:
            return parse_json(r.text)  
        else:
            raise TaskNotFoundError(id)

    @classmethod
    @connection_error_handle
    def read_all(cls):
        task_json = json.loads(requests.get(url()).text)
        return rx.Observable.from_list(task_json).map(lambda t: Task(**taskSchema.load(t)))

    @classmethod
    @connection_error_handle
    def read_state(cls, state: int):
        task_json = json.loads(requests.get(url()+f"?state={state}").text)
        return rx.Observable.from_list(task_json).map(lambda t: Task(**taskSchema.load(t)))

    @classmethod
    @connection_error_handle
    def read_from_list(cls, ids: List[int]):
        def query(observer):
            for id in ids:
                observer.on_next(cls.read(id))
            observer.on_completed()
        return rx.Observable.create(query)

    @classmethod
    @connection_error_handle
    def patch(cls, task_id: int, patch: dict):
        r = requests.patch(url(task_id), json=patch)
        if r.status_code == 404:
            raise TaskNotFoundError(task_id)

    @classmethod
    @connection_error_handle
    def task_slurm_depends_checking(cls, task_slurm_ids: List[int]):
        # TODO Refactor: implementation is not elegant
        import numpy as np

        task_ids = []
        for task_slurm_id in task_slurm_ids:
            task_slurm = requests.get(url_taskSlurm(task_slurm_id)).text
            task_ids.append(json.loads(task_slurm)['task_id'])

        depends_tasks = cls.read_from_list(task_ids).to_list().to_blocking().first()

        state_buf = []
        for i in depends_tasks:
            state_buf.append(i['state'])

        return all(np.ones(len(state_buf))*TaskState.Completed.value == state_buf)

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