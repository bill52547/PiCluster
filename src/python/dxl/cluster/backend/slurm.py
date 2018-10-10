import re
from enum import Enum
from typing import Dict, Iterable
import numpy as np
import rx
from dxl.cluster.web.urls import req_url
import requests

from jfs.directory import Directory
from jfs.file import File

import json
import time

from ..interactive.base import Task, State, Type, Worker, TaskInfo
from ..interactive import web
from .base import Cluster


# from .forcluster import scancel,sbatch,squeue

def scontrol_url(tid):
    return f'http://www.tech-pi.com:1888/api/v1/slurm/scontrol?job_id={tid}'


def scancel_url(tid):
    return 'http://www.tech-pi.com:1888/api/v1/slurm/scancel?job_id={}'.format(tid)


def squeue_url():
    return 'http://www.tech-pi.com:1888/api/v1/slurm/squeue'


def sbatch_url(sargs, file, work_directory):
    return f'http://www.tech-pi.com:1888/api/v1/slurm/sbatch?arg={sargs}&file={file}&work_dir={work_directory}'


class SlurmStatue(Enum):
    Running = 'R'
    Completing = 'CG'
    Completed = 'E'
    Pending = 'PD'
    Failed = 'F'


def slurm_statue2task_statue(s: SlurmStatue):
    return {
        SlurmStatue.Running: State.Runing,
        SlurmStatue.Completing: State.Complete,
        SlurmStatue.Completed: State.Complete,
        SlurmStatue.Pending: State.Pending,
        SlurmStatue.Failed: State.Failed
    }[s]


class ScontrolStatue(Enum):
    Pending = 'PENDING'
    Running = 'RUNNING'
    Suspended = 'SUSPENDED'
    Complete = 'COMPLETED'
    Failed = 'FAILED'
    Canceled = 'CANCELLED'
    Timeout = 'TIMEOUT'
    NodeFailed = 'NODE_FAILED'


def scontrol_statue2task_statue(s: ScontrolStatue):
    scontrol2state_mapping = {
        ScontrolStatue.Pending: State.Pending,
        ScontrolStatue.Running: State.Runing,
        ScontrolStatue.Suspended: State.Runing,
        ScontrolStatue.Complete: State.Complete,
        ScontrolStatue.Failed: State.Failed,
        ScontrolStatue.Canceled: State.Failed,
        ScontrolStatue.Timeout: State.Failed,
        ScontrolStatue.NodeFailed: State.Failed
    }
    return scontrol2state_mapping[ScontrolStatue(s)]


class TaskSlurmInfo:
    def __init__(self, partition=None, command=None, usr=None,
                 statue=None,
                 run_time=None,
                 nb_nodes=None,
                 node_list=None,
                 sid=None):
        self.sid = sid
        if isinstance(self.sid, str):
            self.sid = int(self.sid)
        self.partition = partition
        self.command = command
        self.usr = usr
        if statue == None:
            self.statue = SlurmStatue('R')
        elif isinstance(statue, SlurmStatue):
            self.statue = statue
        else:
            self.statue = SlurmStatue(statue)
        self.run_time = run_time
        if nb_nodes == None:
            self.nb_nodes = 0
        else:
            self.nb_nodes = int(nb_nodes)
        self.node_list = node_list

    # self.depens = depens

    def __eq__(self, m):
        return isinstance(m, TaskSlurmInfo) and m.unbox() == self.unbox()

    def unbox(self):
        return self.sid, self.partition, self.command, self.usr, self.statue, self.run_time, self.nb_nodes, self.node_list

    @classmethod
    def parse_dict(cls, dct: dict):
        return TaskSlurmInfo(dct['partition'],
                             dct['name'],
                             dct['user'],
                             dct['status'],
                             dct['time'],
                             dct['nodes'],
                             dct['node_list'],
                             sid=dct['job_id'])

    def to_dict(self) -> Dict[str, str]:
        return {
            'job_id': self.sid,
            'partition': self.partition,
            'name': self.command,
            'user': self.usr,
            'status': self.statue.value,
            'time': self.run_time,
            'nodes': self.nb_nodes,
            'node_list': self.node_list
        }

    def __repr__(self):
        return f'taskslurm(sid={self.sid})'


class TaskSlurm(Task):
    def __init__(self, script_file, info=None, tid=None, desc='',
                 workdir='.', father=None, statue=None, time_stamp=None,
                 dependency=None, is_root=True, data=None, ttype=Type.Script):
        super().__init__(tid=tid, desc=desc, workdir=workdir, worker=Worker.Slurm, father=father, ttype=ttype,
                         state=statue, time_stamp=time_stamp, dependency=dependency, is_root=is_root, data=data,
                         script_file=script_file, info=info)

    @property
    def sid(self):
        return self.info['job_id']


def sid_from_submit(s: str):
    return int(re.sub('\s+', ' ', s).strip().split(' ')[3])


def squeue() -> 'Observable[TaskSlurmInfo]':
    infos = requests.get(squeue_url()).text
    info = json.loads(infos)
    return (rx.Observable.from_(info)
            .map(lambda l: TaskSlurmInfo.parse_dict(l))
            .filter(lambda l: l is not None))


def sbatch(workdir: Directory, filename, args):
    result = requests.post(sbatch_url(args, filename, workdir)).json()
    return result['job_id']


def scancel(sid: int):
    if sid is None:
        return False
    requests.delete(scancel_url(sid))


def scontrol(sid: int):
    if sid is None:
        return False
    result = requests.get(scontrol_url(sid)).json()
    return result


def get_statue(sid: int):
    if sid is None:
        return False
    state = scontrol_statue2task_statue(scontrol(sid)['job_state'])
    return state


def find_sid(sid):
    return lambda tinfo: int(tinfo.sid) == int(sid)


def is_end(sid):
    if sid is None:
        return False
    result = (squeue()
              .filter(find_sid(sid))
              .count().to_list().to_blocking().first())
    return result[0] == SlurmStatue.Completed


def is_complete(sid):
    return is_end(sid)


def get_task_info(sid: int) -> TaskSlurmInfo:
    result = squeue().filter(find_sid(sid)).to_list().to_blocking().first()
    if len(result) == 0:
        return None
    return result[0]


class Slurm(Cluster):
    @classmethod
    def submit(cls, t: TaskSlurm):
        sid = sbatch(t.workdir, t.script_file[0], t.info['args'])
        slurm_info = get_task_info(sid)
        new_info = TaskInfo(sid=sid, nb_nodes=slurm_info.nb_nodes, node_list=slurm_info.node_list,
                            nb_GPU=t.info['GPUs'], args=t.info['args'])
        # new_task = t.update_info(new_info.to_dict())
        new_task = t.update_info(new_info.to_dict())
        nt = new_task.update_state(State.Runing)
        # nt = nt.update_start()
        web.Request().update(nt)
        return nt

    @classmethod
    def update(cls, t: TaskSlurm):
        if t.info['job_id'] is None:
            return t
        else:
            state = get_statue(t.info['job_id'])
            nt = t.update_state(state)
            return nt

    @classmethod
    def cancel(cls, t: TaskSlurm):
        """
        取消任务
        """
        scancel(t.sid)
