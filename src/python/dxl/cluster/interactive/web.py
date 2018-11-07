import json
from functools import wraps
import requests
import rx

from .exceptions import TaskDatabaseConnectionError, TaskNotFoundError
from ..config import config
# from .base import Task
from ..database2.model import TaskState, Task, TaskSlurm, TaskSimu
from typing import List
from ..database2.model import taskSchema, taskSlurmSchema, TaskSlurm
from functools import singledispatch
import json

import datetime

#TODO move to config file
REQUEST_IP = "localhost"

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
        return req_url(config['name'], REQUEST_IP, config['port'], None, config['version'], config['base'])
    else:
        return req_url(config['name'], REQUEST_IP, config['port'], id, config['version'], config['base'])

def url_taskSlurm(id=None):
    if id is None:
        return req_url(ip=REQUEST_IP,
                       port=config['port'],
                       version=config['version'],
                       name="taskslurm",
                       suffix=None)
    else:
        return req_url(ip=REQUEST_IP,
                       port=config['port'],
                       version=config['version'],
                       name="taskslurm",
                       suffix=id)

def url_jointask(id=None):
    if id is None:
        return req_url(ip=REQUEST_IP,
                       port=config['port'],
                       version=config['version'],
                       name="jointask",
                       suffix=None)
    else:
        return req_url(ip=REQUEST_IP,
                       port=config['port'],
                       version=config['version'],
                       name="jointask",
                       suffix=id)


API_BASE = "http://192.168.1.185:3000"
TASK_QUERY = "task?"
TASKSLURM_QUERY = "taskSlurm?"


# @singledispatch
# def url(task):
#     raise TypeError(f"Url factory for type {type(task)} not implemented..")
#
#
# @url.register(Task)
# def _(task):
#     return API_BASE+f"/tasks?and=(id.eq.{task.id})"
#
#
# @url.register(TaskSlurm)
# def _(taskSlurm):
#     return API_BASE + f"/taskSlurm?and=(id.eq.{taskSlurm.id})"


def parse_json(s: 'json string'):
    return Task.from_json(s)

    
class Request:

    # @classmethod
    # def to_json(cls, text):
    #     return json.loads(text.strip("[]"))

    @classmethod
    @connection_error_handle
    def create(cls, task):
        task_json = task.to_json()
        r = requests.post(url(), json=task_json).json()
        task.id = r['id']
        return task

    @classmethod
    @connection_error_handle
    def read(cls, id: int):
        result = requests.get(API_BASE + f"/tasks?and=(id.eq.{id})")
        return Task(**taskSchema.load(json.loads(result.text)[0]))

    @classmethod
    @connection_error_handle
    def read_all(cls):
        task_json = json.loads(requests.get(url()).text)
        return rx.Observable.from_list(task_json).map(lambda t: Task(**taskSchema.load(t)))

    # @classmethod
    # @connection_error_handle
    # def read_state(cls, state: int):
    #     task_json = json.loads(requests.get(url()+f"?state={state}").text)
    #     return rx.Observable.from_list(task_json).map(lambda t: Task(**taskSchema.load(t)))

    @classmethod
    @connection_error_handle
    def read_state(cls, state: int):
        # task_json = json.loads(requests.get(url()+f"?state={state}").text)
        task_json = json.loads(requests.get(API_BASE + f'/tasks?and=(state.eq.{TaskState(state).name})').text)
        return rx.Observable.from_list(task_json).map(lambda t: Task(**taskSchema.load(t)))

    @classmethod
    @connection_error_handle
    def read_from_list(cls, ids: List[int]):
        """
        Get a Task obj stream with given list of ids.
        :param ids:
        :return:
        """
        def query(observer):
            for id in ids:
                observer.on_next(cls.read(id))
            observer.on_completed()
        return rx.Observable.create(query)

    @classmethod
    @connection_error_handle
    def read_taskSlurm(cls, taskSlurm_id: int):
        def query(observer):
            observer.on_next(json.loads(requests.get(url_taskSlurm(taskSlurm_id)).text))
            observer.on_completed()
        return rx.Observable.create(query).map(lambda t: TaskSlurm(**taskSlurmSchema.load(t)))

    @classmethod
    @connection_error_handle
    def patch(cls, task_id: int, patch: dict):
        r = requests.patch(url(task_id), json=patch)
        if r.status_code == 404:
            raise TaskNotFoundError(task_id)

    @classmethod
    @connection_error_handle
    def taskSlurm_patch(cls, id: int, patch: dict):
        r = requests.patch(url_taskSlurm(id), json=patch)
        if r.status_code == 404:
            raise TaskNotFoundError(id)

    @classmethod
    @connection_error_handle
    def number_of_pending_depends(cls, task_slurm_ids: List[int]):
        """
        List of TaskSlurm id ->
        List of corresponding Task objects ->
        Sum up Task.state != TaskState.Completed.value

        The 0 result of this the streaming indicates that all depends completed. And the task is submittable.
        :param task_slurm_ids:
        :return:
        """
        buf = list()
        for task_slurm_id in task_slurm_ids:
            buf.append(cls.read_taskSlurm(task_slurm_id))

        return (rx.Observable.from_(buf).merge_all()
                .map(lambda t: t.task_id)
                .map(lambda task_id: Request.read(task_id).state.value)
                .map(lambda i: i != TaskState.Completed.value).sum())

    @classmethod
    @connection_error_handle
    def reverse_cross_query(cls, taskSlurm: TaskSlurm):
        """
        Query Task obj using TaskSlurm obj.
        :param taskSlurm:
        :return:
        """
        def query(observer):
            observer.on_next(json.loads(requests.get(url(taskSlurm.task_id)).text))
        return rx.Observable.create(query).map(lambda t: Task(**taskSchema.load(t)))


    @classmethod
    @connection_error_handle
    def cross_query(cls, task: Task):
        """
        Query TaskSlurm by Task.
        :param task:
        :return:
        """
        def query(observer):
            observer.on_next(json.loads(requests.get(url_jointask(task.id)).text))
            observer.on_completed()
        return rx.Observable.create(query).map(lambda t: TaskSlurm(**taskSlurmSchema.load(t)))

    @classmethod
    @connection_error_handle
    def delete(cls, id):
        r = requests.delete(url(id))
        if r.status_code == 404:
            raise TaskNotFoundError(id)