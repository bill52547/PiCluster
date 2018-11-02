from .cluster import submit_slurm
from .interactive.web import Request
from .interactive.base import Task, TaskInfo, Worker, State, Type
from .backend.slurm import TaskSlurm 

def submit_task(task):
    # print(task.to_json())
    return Request().create(task)
    # return Request.create(Request(), task)

def read_task(tid):
    return Request().read(tid)

def read_all():
    return Request().read_all()

def update_task(task):
    return Request().update(task)

def delete_task(tid):
    return Request().delete(tid)    
