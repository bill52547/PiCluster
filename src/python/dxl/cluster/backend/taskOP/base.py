import yaml
import rx
from rx import Observable
import time
import shutil
from pathlib import Path
from functools import partial
import subprocess


# simple_callback = lambda: "Done"


# def op_factory(op):
#     return rx.Observable.defer(lambda: rx.Observable.start(op))


# def op_repeat(task, n):
#     if n == 1:
#         return task
#     return task.switch_map(lambda x: op_repeat(task, n-1))
#
#
# def op_join(task1, task2):
#     return task1.switch_map(lambda x: task2)


# def run_script(cmd, workdir, callback=simple_callback):
#     subprocess.run(cmd, cwd=workdir)
#     return callback


# def load(src, workdir, callback=simple_callback):
#     if isinstance(src, str):
#         src = Path(src)
#     if isinstance(src, Path) and src.is_file():
#         if isinstance(workdir, str):
#             workdir = Path(workdir)
#         shutil.copyfile(src=src, dst=workdir/src.name)
#     return callback


# def sleep_1():
#     print("yo! +1s")
#     time.sleep(1)
#     return "+1s done!"