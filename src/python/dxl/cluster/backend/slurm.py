import re
from enum import Enum
from typing import Dict, Iterable

import rx

from dxl.fs import Directory, File

from ..task import Task, TaskStatue
from .base import Cluster


class SlurmStatue(Enum):
    Running = 'R'
    Completing = 'CG'
    Completed = 'E'


def slurm_statue2task_statue(s: SlurmStatue):
    return {
        SlurmStatue.Running: TaskStatue.Running,
        SlurmStatue.Completing: TaskStatue.Running
    }


class TaskSlurmInfo:
    def __init__(self,
                 sid: int, partition: str, command: str, usr: str,
                 statue: SlurmStatue,
                 run_time: str,
                 nb_nodes: int,
                 node_list: Iterable[str]):
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

    @classmethod
    def parse_dict(cls, dct):
        return cls(dct.get('sid'),
                   dct.get('partition'),
                   dct.get('command'),
                   dct.get('usr'),
                   dct.get('statue'),
                   dct.get('run_time'),
                   dct.get('nb_nodes'),
                   dct.get('node_list'))

    def to_dict(self) -> Dict[str, str]:
        return {
            'sid': self.sid,
            'partition': self.partition,
            'command': self.command,
            'usr': self.usr,
            'statue': self.statue.value,
            'run_time': self.run_time,
            'nb_nodes': self.nb_nodes,
            'node_list': self.node_list
        }


class TaskSlurm(Task):
    def __init__(self, script_file: File, work_directory: Directory,
                 statue: TaskStatue=None, info: Dict[str, str]=None,
                 tid: int=None):
        super().__init__(work_directory, Slurm(), statue, info, tid)
        self.script_file = script_file

    @property
    def sid(self):
        return self.info.get('sid')

    def add_depens(self, sids: Iterable[int]):
        new_info = dict(self.info)
        new_info['depens'] = tuple(sids)
        return self.update_info(new_info)

    def update_info(self, new_info: Dict[str, str]):
        return TaskSlurm(self.script_file, self.work_directory,
                         self.statue, self.info,
                         self.tid)

    def update_statue(self, new_statue: TaskStatue):
        return TaskSlurm(self.script_file, self.work_directory,
                         new_statue, self.info, self.tid)


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
    cmd = 'cd {dir} && sbatch {file}'.format(dir=workdir.system_path(),
                                             args=' '.join(args),
                                             file=script_file.system_path())
    result = _apply_command(cmd)
    return sid_from_submit(result[0])


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
    deps = t.info.get('depens')
    if deps is None:
        return ()
    else:
        return ['--dependency=afterok:'] + ':'.join(deps)


class Slurm(Cluster):

    @classmethod
    def submit(cls, t: TaskSlurm):
        sid = cls.sbatch(t.work_directory, t.script_file, *dependency_args(t))
        info = dict(t.info)
        info['sid'] = sid
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
