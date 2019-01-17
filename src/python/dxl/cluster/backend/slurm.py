import rx
import json
import yaml
import requests
import shutil
from jfs.directory import Directory
from pathlib import Path

from .base import AutoName
from enum import auto
from ..config.urls import req_slurm
from ..interactive.web import Request
from ..interactive.templates import env, master_task_on_slurm


class SlurmOp(AutoName):
    scontrol = auto()
    scancel = auto()
    squeue = auto()
    sbatch = auto()


def squeue():
    response = requests.get(req_slurm(SlurmOp.squeue.value)).text
    return json.loads(response)


def sbatch(work_dir: Directory, file):
    arg = file
    _url = req_slurm(SlurmOp.sbatch.value, arg=arg, file=file, work_dir=work_dir)
    print(_url)
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


def config_parser(config_dict):
    tmp_outer = []
    for k, v in config_dict.items():
        tmp_inner = []
        tmp_inner.append(k)
        if isinstance(v, dict):
            for k, v_1 in v.items():
                tmp_inner.append(k)
                tmp_inner.append(v_1)
        tmp_outer.append(tmp_inner)
    return tmp_outer


def url_parser(query_config):
    all_inputs = []

    for row in query_config:
        response = Request.read(table_name=row[0],
                                item=row[1],
                                condition=row[2],
                                returns=row[4])
        for r in response['data'][row[0]]:
            for k, v in r.items():
                all_inputs.append(v)
    return all_inputs


def input_loading(workdir, source):
    if not isinstance(workdir):
        workdir = Path(workdir)
    for f in source:
        if isinstance(f, Path):
            shutil.copyfile(f, workdir/f.name)
        if isinstance(f, str):
            f = Path(f)
            shutil.copyfile(f, workdir/f.name)
