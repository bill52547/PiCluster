from dxl.fs import Directory, File
from typing import Iterable
from .backend import slurm
from .database2.model import TaskState


def submit_slurm(workdir: Directory, script: File, depends: Iterable[int]=()) -> int:
    t = slurm.TaskSlurm(details={"script": script,
                                 "workdir": workdir},
                        state=TaskState.BeforeSubmit,
                        depends=depends)
    t_submitted = slurm.Slurm.submit(t)
    return t_submitted.task_id