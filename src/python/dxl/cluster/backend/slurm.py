import re
from enum import Enum
from typing import Dict, Iterable

import rx

from dxl.fs import Directory, File

import json

from ..interactive.base import Task, State,Type
from .base import Cluster


class SlurmStatue(Enum):
    Running = 'R'
    Completing = 'CG'
    Completed = 'E'
    Pending = 'PD'


def slurm_statue2task_statue(s: SlurmStatue):
    return {
        SlurmStatue.Running: State.Running,
        SlurmStatue.Completing: State.Running,
        SlurmStatue.Completed: State.Complete,
        SlurmStatue.Pending: State.Pending,
    }[s]


class TaskSlurmInfo:
    def __init__(self,
                 sid: int, partition: str, command: str, usr: str,
                 statue: SlurmStatue,
                 run_time: str,
                 nb_nodes: int,
                 node_list: Iterable[str],
                 depens:tuple):
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
        self.depens = depens

    @classmethod
    def parse_dict(cls, dct):
        return cls(dct.get('sid'),
                   dct.get('partition'),
                   dct.get('command'),
                   dct.get('usr'),
                   dct.get('statue'),
                   dct.get('run_time'),
                   dct.get('nb_nodes'),
                   dct.get('node_list'),
                   dct.get('depens'))

    def to_dict(self) -> Dict[str, str]:
        return {
            'sid': self.sid,
            'partition': self.partition,
            'command': self.command,
            'usr': self.usr,
            'statue': self.statue.value,
            'run_time': self.run_time,
            'nb_nodes': self.nb_nodes,
            'node_list': self.node_list,
            'depens': self.depens
        }


class TaskSlurm(Task):
    def __init__(self, tid: int=None,desc=None,work_directory=None,worker='Slurm',father=None,
                     ttype=Type.Script,statue=None,time_stamp=None,dependency=None,is_root=True,
                     data=None,script_file:File,info:TaskSlurmInfo):
        self.script_file = script_file
        self.info = info   
        data['script_file']= self.script_file.to_serializable()
        data['info'] = self.info.to_dict()
        # data = json.dumps(datainfo)
        super().__init__(tid=tid,desc=desc,workdir=work_directory, worker=worker,father=father,ttype=ttype,
                         state=statue, time_stamp=time_stamp,dependency=dependency,is_root=is_root,data=data)
        

    @property
    def sid(self):
        return self.info.get('sid')

    def add_depens(self, sids: Iterable[int]):
        new_info = dict(self.info)
        new_info['depens'] = tuple(sids)
        return self.update_info(new_info)


    def update_info(self, new_info: Dict[str, str]):
        return TaskSlurm(tid=self.id,desc=self.desc,work_directory=self.workdir,worker=self.worker,father=self.father,
                 ttype=self.ttype,statue=self.state,time_stamp=self.time_stamp,dependency=self.dependency,
                 is_root=self.is_root,data=self.data,script_file=self.script_file, info=new_info)

    def update_statue(self, new_statue: State):
        return TaskSlurm(tid=self.id,desc=self.desc,work_directory=self.workdir,worker=self.worker,father=self.father,
                 ttype=self.ttype,statue=new_statue,time_stamp=self.time_stamp,dependency=self.dependency,
                 is_root=self.is_root,data=self.data,script_file=self.script_file, info=self.info)


def _apply_command(command) -> Iterable[str]:
    """
    Parameters:

    - `command`: shell command str.

    Returns:

    - std.out in lines
    """
    import os
    with os.popen(command) as fin:
        return fin.readlines()


def sid_from_submit(s: str):
    return int(re.sub('\s+', ' ', s).strip().split(' ')[3])


def task_info_from_squeue(s: str):
    s = re.sub('\s+', ' ', s).strip()
    items = s.split()
    if not items[0].isdigit():
        return None
    else:
        return TaskSlurmInfo(*items)


def squeue() -> 'Observable[TaskSlurmInfo]':
    return (rx.Observable.from_(_apply_command('squeue'),
                                scheduler=rx.concurrency.ThreadPoolScheduler())
            .map(lambda l: task_info_from_squeue(l))
            .filter(lambda l: l is not None))


def sbatch(workdir: Directory, script_file: File, *args):
    sargs = ' '.join(args)
    if sargs != '' and (not sargs.endswith(' ')):
        sargs += ' '
    cmd = 'cd {dir} && sbatch {args}{file}'.format(dir=workdir.system_path(),
                                             args=sargs,
                                             file=script_file.system_path())
    result = _apply_command(cmd)
    return sid_from_submit(result[0])

def scancel(sid):
    if sid is None:
        return False
    cmd = 'scancel {sid}'.format(sid=sid)
    _apply_command(cmd)

# def get_info(sid):
#     if sid is None:
#         return False
#     cmd = 'scontrol show job {tid}'.format(sid=sid)
#     result = _apply_command(cmd)   #字符串需解析
#     return result

def find_sid(sid):
    return lambda tinfo: tinfo.sid == sid


def is_end(sid):
    if sid is None:
        return False
    result = (squeue()
              .filter(lambda tinfo: tinfo.sid == sid)
              .count().to_list().to_blocking().first())
    return result[0] == 0


def is_complete(sid):
    return is_end(sid)


def dependency_args(t: TaskSlurm) -> TaskSlurm:
    deps = t.dependency
    if deps is None or len(deps) == 0:
        return ()
    else:
        return ('--dependency=afterok:' + ':'.join(map(str, deps)),)


def get_task_info(sid: int) -> TaskSlurmInfo:
    result = squeue().filter(find_sid(sid)).to_list().to_blocking().first()
    if len(result) == 0:
        return None
    return result[0]


class Slurm(Cluster):
    @classmethod
    def submit(cls, t: TaskSlurm):
        sid = sbatch(t.workdir, t.script_file, *(dependency_args(t)))
        info = dict(t.info)
        new_info = get_task_info(sid).to_dict()
        info.update(new_info)
        new_task = t.update_info(info)
        return cls.update(new_task)

    @classmethod
    def update(self, t: TaskSlurm):
        if t.sid is None:
            return t
        else:
            new_info = (squeue().filter(lambda tinfo: tinfo.sid == t.sid)
                        .to_list().to_blocking().first())
            if len(new_info) == 0:
                new_info = TaskSlurmInfo.parse_dict(t.info)
                new_info.statue = SlurmStatue.Completed
            else:
                new_info = new_info[0]
        return (t.update_info(new_info.to_dict())
                .update_statue(slurm_statue2task_statue(new_info.statue)))
    
    @classmethod
    def cancel(self, t: TaskSlurm):
        scancel(t.sid)
        #return t

    @classmethod
    def is_failure(self,t:TaskSlurm):
        result = get_task_info(t.sid)
        if result[3] == failure:
            state = State.Failed
            return t.update_statue(state)
        else:
            return t


