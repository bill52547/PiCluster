import json
from functools import wraps

import requests
import rx

from .exceptions import TaskDatabaseConnectionError, TaskNotFoundError


def connection_error_handle(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.ConnectionError as e:
            raise TaskDatabaseConnectionError(
                "Task database server connection failed. Details:\n{e}".format(e=e))
    return wrapper


def req_url(tid=None):
    from .config import config as c
    from dxpy.web.urls import req_url
    if tid is None:
        return req_url(c['names'], c['ip'], c['port'], None, c['version'], c['base'])
    else:
        return req_url(c['name'], c['ip'], c['port'], tid, c['version'], c['base'])


@connection_error_handle
def create(task_json):
    r = requests.post(req_url(), {'task': task_json})
    return r.json()['id']



@connection_error_handle
def read(tid):
    r = requests.get(req_url(tid))
    if r.status_code == 200:
        return r.text
    else:
        raise TaskNotFoundError(tid)


@connection_error_handle
def read_all():
    return rx.Observable.from_(json.loads(requests.get(req_url()).text))


@connection_error_handle
def update(task_json):
    task_json_dct = json.loads(task_json)
    r = requests.put(req_url(
        task_json_dct['id']), data={'task': task_json})
    if r.status_code == 404:
        raise TaskNotFoundError(task_json_dct['id'])


@connection_error_handle
def delete(tid):
    r = requests.delete(req_url(tid))
    if r.status_code == 404:
        raise TaskNotFoundError(tid)
