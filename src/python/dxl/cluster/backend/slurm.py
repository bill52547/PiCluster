import re
from enum import Enum
from typing import Dict, Iterable
import numpy as np
import rx
from dxpy.web.urls import req_url
import requests

from dxpy.filesystem import Directory, File

import json

from ..interactive.base import Task,State,Type,Worker
from ..interactive import web
from .base import Cluster
#from .forcluster import scancel,sbatch,squeue

def scontrol_url(tid):
    return f'http://www.tech-pi.com:1888/api/v1/slurm/scontrol?job_id={tid}'

def scancel_url(tid):
    return 'http://www.tech-pi.com:1888/api/v1/slurm/scancel?job_id={}'.format(tid)

def squeue_url():
    return 'http://www.tech-pi.com:1888/api/v1/slurm/squeue'

def sbatch_url(sargs,work_directory):
    return f'http://www.tech-pi.com:1888/api/v1/slurm/sbatch?arg&file={sargs}&work_dir={work_directory}'


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


class TaskSlurmInfo:
    def __init__(self, partition: str, command: str, usr: str,
                 statue: SlurmStatue,
                 run_time: str,
                 nb_nodes: int,
                 node_list: Iterable[str],
                 sid=None):
        self.sid = sid
        if isinstance(self.sid, str):
            self.sid = int(self.sid)
        self.partition = partition
        self.command = command
        self.usr = usr
        if isinstance(statue, SlurmStatue):
            self.statue = statue
        else:
            self.statue = SlurmStatue(statue)
        self.run_time = run_time
        self.nb_nodes = int(nb_nodes)
        self.node_list = node_list
        #self.depens = depens

    @classmethod
    # def parse_dict(cls, dct):
    #     return cls(dct.get('sid'),
    #                dct.get('partition'),
    #                dct.get('command'),
    #                dct.get('usr'),
    #                dct.get('statue'),
    #                dct.get('run_time'),
    #                dct.get('nb_nodes'),
    #                dct.get('node_list'),
    #                dct.get('depens'))
    def parse_dict(cls, dct:str):
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
    def __init__(self,script_file,info=None,tid=None,desc='',
        workdir='.',father=None,statue=None,time_stamp=None,
        dependency=None,is_root=True,data=None,ttype=Type.Script):  
        super().__init__(tid=tid,desc=desc,workdir=workdir, worker=Worker.Slurm,father=father,ttype=ttype,
                         state=statue, time_stamp=time_stamp,dependency=dependency,is_root=is_root,data=data,
                         script_file=script_file,info=info)
        

    @property
    def sid(self):
        return self.info.sid

    
    # def update_info(self, new_info):
    #     return TaskSlurm(tid=self.id,desc=self.desc,work_directory=self.workdir,worker=self.worker,father=self.father,
    #              ttype=self.ttype,statue=self.state,time_stamp=self.time_stamp,dependency=self.dependency,
    #              is_root=self.is_root,data=self.data,script_file=self.script_file, info=new_info)

    
def sid_from_submit(s: str):
    return int(re.sub('\s+', ' ', s).strip().split(' ')[3])


# def task_info_from_squeue(s: str):
#     s = re.sub('\s+', ' ', s).strip()
#     items = s.split()
#     if not items[0].isdigit():
#         return None
#     else:
#         return TaskSlurmInfo(*items)

def squeue() -> 'Observable[TaskSlurmInfo]':
    infos = requests.get(squeue_url()).text
    info = json.loads(infos)
    return (rx.Observable.from_(info)
          .map(lambda l:TaskSlurmInfo.parse_dict(l))
          .filter(lambda l:l is not None))


def sbatch(workdir: Directory, args):
    # sargs = ' '.join(args)
    # if sargs != '' and (not sargs.endswith(' ')):
    #     sargs += ' '
    result = requests.post(sbatch_url(args,workdir)).json()
    return result['job_id']

def scancel(sid:int):
    if sid is None:
        return False
    requests.delete(scancel_url(sid))

def scontrol(sid:int):
    if sid is None:
        return False
    result = requests.get(scontrol_url(sid)).json()
    return result

def get_statue(sid):
    if sid is None:
        return False
    state = scontrol(sid)['job_state']
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
        sid = sbatch(t.workdir, t.script_file[0])
        new_info = get_task_info(sid) 
        new_task = t.update_info(new_info.to_dict())
        t.update_state(slurm_statue2task_statue(new_info.statue))
        web.Request().update(t)
        return new_task
        

    # @classmethod
    # def update(cls, t: TaskSlurm):
    #     """
    #     从slurm中读取状态并返回给task系统
    #     """
    #     if t.sid is None:
    #         return False
    #     else:
    #         new_info = (squeue().filter(lambda tinfo: tinfo.sid == t.sid)
    #                     .to_list().to_blocking().first())
    #         if len(new_info) == 0:
    #             new_info = TaskSlurmInfo.parse_dict(t.info)
    #             new_info.statue = SlurmStatue.Completed
    #             t.update_info(new_info.to_dict())
    #             t.update_state(slurm_statue2task_statue(new_info.statue))
    #         else:
    #             new_info = new_info[0]
    #             t.update_info(new_info.to_dict())
    #             t.update_state(slurm_statue2task_statue(new_info.statue))
    #             t.father.update_state(slurm_statue2task_statue(new_info.statue))
    #             #web.Request().update(t)
    @classmethod
    def update(cls,t:TaskSlurm):
        if t.info['job_id'] is None:
            return False
        else:
            new_info = get_task_info(t.info['job_id'])
            print(new_info)
            if new_info is not None: 
                t.update_state(slurm_statue2task_statue(new_info.statue))
            else:
                state = scontrol(t.info['job_id'])['job_state']
                print(state)
                if state!='COMPLETED':
                    t.update_state(State.Failed)
            return t


    @classmethod
    def cancel(cls, t: TaskSlurm):
        """
        取消任务
        """
        scancel(t.sid)
        #return t

    # @classmethod
    # def is_failure(cls,t:TaskSlurm):
    #     result = get_task_info(t.sid)
    #     if result[3] == failure:
    #         state = State.Failed
    #         return t.update_statue(state)
    #     else:
    #         return t


