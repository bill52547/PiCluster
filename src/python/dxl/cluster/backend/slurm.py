import rx
import yaml
import json
import requests
from jfs.directory import Directory

from .base import AutoName
from enum import auto
from ..config.urls import req_slurm
# from ..database.tasks import TaskParser, ps
# from ..database.transactions import deserialization, serialization
# from ..database.model import Mastertask
# from . import Backends


class SlurmOp(AutoName):
    scontrol = auto()
    scancel = auto()
    squeue = auto()
    sbatch = auto()


def squeue():
    response = requests.get(req_slurm(SlurmOp.squeue.value)).text
    return json.loads(response)


def sbatch(workdir: Directory, filename):
    args = filename
    _url = req_slurm(SlurmOp.sbatch.value, args=args, filename=filename, workdir=workdir)
    result = requests.post(_url).json()
    return result['job_id']


def scancel(id: int):
    if id is None:
        raise ValueError
    requests.delete(req_slurm(SlurmOp.scancel.value, job_id=id))


def scontrol(id: int):
    def query(observer):
        try:
            result = requests.get(req_slurm(SlurmOp.scontrol.value, job_id=id)).json()
            observer.on_next(result['job_state'])
            observer.on_completed()
        except:
            pass

    return rx.Observable.create(query)


# @TaskParser.post.register(self.backend = Backends.slurm)
# def _(self):
#     task = deserialization(self.task_body)
#     ps(task)
#     # task = TasksBind.tasks.post(task)
#
#     slurmTask = deserialization(self.task_details)
#     ps(slurmTask)
#     # slurmTask.task_id = task.id
#     # slurmTask = TasksBind.tasks.post(slurmTask, task.id)
#
#     if self.is_master_task:
#         # masterTask = TasksBind.tasks.post(Mastertask(backend=Backends.slurm.value, id=slurmTask.id))
#         real_ps = ps(Mastertask(backend=Backends.slurm.value, id=slurmTask.id))
#         real_ps.subscribe(print)
#         return serialization(Mastertask(backend=Backends.slurm.value, id=0)), 200
#
#     return serialization(slurmTask), 200


