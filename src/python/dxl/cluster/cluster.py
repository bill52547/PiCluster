from dxl.fs import Directory, File
from typing import Iterable
from .backend import slurm
from .task import TaskStatue


def submit_slurm(work_directory: Directory, script_file: File, depens: Iterable[int]=()) -> int:
    t = slurm.TaskSlurm(script_file, work_directory,
                        TaskStatue.BeforeSubmit,
                        info={'depens': depens})
    t_submitted = slurm.Slurm.submit(t)
    return t_submitted.sid
