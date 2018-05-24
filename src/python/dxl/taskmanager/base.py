"""
This moudule is designed to be stateless (pure interface):
    No data members
    No Deamon process
Works
"""
import copy
import rx
import json
import rx
from dxpy.time.utils import now
from dxpy.time.timestamps import TaskStamp
from .. import database
from ..database.service import task as ts


def create(task) -> int:
    return database.create(task.to_json())


def parse_json(s: 'json string'):
    return ts.Task.from_json(s)


def read(tid: int):
    if not isinstance(tid, int):
        raise TypeError("read only accept tid of int type: {!r}".format(tid))
    return parse_json(database.read(tid))


def read_all() -> 'Observable<TaskPy>':
    return (database.read_all()
            .map(parse_json))


def delete(tid: int) -> None:
    database.delete(tid)
