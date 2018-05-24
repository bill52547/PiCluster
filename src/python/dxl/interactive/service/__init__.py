from .base import Service
from .web import create as create_web
from .web import read as read_web
from .web import read_all as read_all_web
from .web import update as update_web
from .web import delete as delete_web
import task


def create_py(task_json):
    return Service.create_new(task_json)


def read_py(tid):
    return Service.read(tid)


def read_all_py():
    return Service.read_all()


def update_py(task_json):
    return Service.update(task_json)


def delete_py(tid):
    return Service.delete(tid)
