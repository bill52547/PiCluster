from enum import Enum
from dxl.fs import Directory
from typing import Dict


class TaskStatue(Enum):
    Unknown = 'Unknown'
    BeforeSubmit = 'BeforeSubmit'
    Pending = 'Pending'
    Running = 'Running'
    Completed = 'Completed'
    Failed = 'Failed'


class Task:
    def __init__(self,
                 work_directory: Directory=None,
                 cluster: 'Cluster'=None,
                 statue: TaskStatue=None,
                 info: Dict[str, str]=None,
                 tid: int=None):
        """
        Parameters:

        - `work_directory`
        - `cluster`: cluster in which task is expected to run or running.
        - `statue` 
        - `info`: a dict which is JSON serializable. 
        - `tid`: universal task id, could be None for un-submitted tasks or anonymous tasks. 
        """
        self.tid = tid
        self.work_directory = work_directory or Directory('.')
        self.cluster = cluster
        self.statue = statue or TaskStatue.Unknown
        self.info = info or {}

    def submit(self) -> 'Task':
        return self.cluster.submit(self)

    def update(self) -> 'Task':
        """
        Update task statue.
        """
        return self.cluster.update(self)

    def update_info(self, new_info) -> 'Task':
        return Task(self.work_directory,
                    self.cluster,
                    self.statue,
                    new_info,
                    self.tid)

    def update_statue(self, new_statue: TaskStatue):
        return Task(self.work_directory,
                    self.cluster,
                    self.statue,
                    new_statue,
                    self.tid)
