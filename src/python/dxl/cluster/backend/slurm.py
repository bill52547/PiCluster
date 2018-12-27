import rx
import json
import requests
from rx import Observable
from jfs.directory import Directory

from .base import AutoName, post
from enum import auto
from ..web.urls import req_slurm
from ..database.model import SlurmTask


class SlurmOp(AutoName):
    scontrol = auto()
    scancel = auto()
    squeue = auto()
    sbatch = auto()



def squeue():
    infos = requests.get(req_slurm(SlurmOp.squeue.value)).text
    info = json.loads(infos)
    return info


def sbatch(workdir: Directory, filename):
    args = filename
    _url = req_slurm(SlurmOp.sbatch.value, args=args, filename=filename, workdir=workdir)
    result = requests.post(_url).json()
    return result['job_id']


def scancel(id: int):
    if id is None:
        return False
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
