"""
Task templates of frequently used ones.
"""
import rx
import os
from dxpy.filesystem import Path
from dxpy.graph.depens import DenpensGraph
from dxpy.exceptions.checks import assert_same_length
from ..database.service import task


def task_graph(tasks, depens):
    assert_same_length((tasks, depens), ('tasks', 'depens'))
    depens_tasks = []
    for i, ds in enumerate(depens):
        if ds is None:
            depens_tasks.append([None])
        elif isinstance(ds, int):
            depens_tasks.append([tasks[ds]])
        else:
            depens_tasks.append([tasks[d] for d in ds])
    g = DenpensGraph(tasks, depens_tasks)
    for t in g.nodes():
        t.is_root = g.is_root(t)
    return g


def task_command(command, worker=None, *args, **kwargs):
    if worker is None:
        worker = task.Worker.MultiThreading
    return task.Task(*args, **kwargs, worker=worker, ttype=task.Type.Command, data={
        'command': command
    })


def task_script(file, *args, **kwargs):
    return task.Task(*args, **kwargs, ttype=task.Type.Script, data={
        'file': (Path(kwargs['workdir']) / file).abs
    })


def task_slurm(file, *args, workdir, **kwargs):
    return task.Task(*args, **kwargs, workdir=workdir, worker=task.Worker.Slurm, ttype=task.Type.Script, data={
        'file': (Path(workdir) / file).abs
    })
