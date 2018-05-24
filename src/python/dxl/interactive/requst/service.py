import sys
import rx
from rx.concurrency import ThreadPoolScheduler
from . import base
from . import workers    ###some new class
#from .. import provider

SUPPORTED_WORKERS = [workers.Slurm, workers.MultiThreding, workers.NoAction]


def start(task):
    workers.get_workers(task).run(task)


def read_complete_tasks_on_worker(worker) -> 'rx.Observable<Task>':
    return (base.read_all()
            .filter(lambda t: t.is_running)
            .filter(worker.on_this_worker)
            .filter(worker.is_complete), )[0]


def auto_complete():
    for w in SUPPORTED_WORKERS:
        tasks = (read_complete_tasks_on_worker(w)
                 .subscribe(base.mark_complete))


def auto_submit_root():
    (base.read_all()
     .filter(lambda t: t.is_before_submit)
     .filter(lambda t: t.is_root)
     .subscribe(base.mark_submit))


def auto_submit_chain():
    (base.read_all()
     .filter(lambda t: t.is_pending)
     .flat_map(base.dependencies)
     .filter(lambda t: t.is_before_submit)
     .subscribe(base.mark_submit))


def is_dependencies_complete(task):
    return (base.dependencies(task)
            .all(lambda t: t.is_complete))


def start(task):
    for w in SUPPORTED_WORKERS:
        if w.on_this_worker(task):
            w.run(task)


def auto_start():
    tasks = (base.read_all()
             .filter(lambda t: t.is_pending))
    (rx.Observable.zip_array(tasks,
                             tasks.flat_map(is_dependencies_complete))
     .filter(lambda x: x[1])
     .map(lambda x: x[0])
     .subscribe(lambda t: start(t)))


# def cycle():
#     auto_complete()
#     auto_submit_root()
#     auto_submit_chain()
#     auto_start()
#     print('Cycle.')


# def launch_deamon(interval=1000):
#     configs = provider.get_or_create_service('config')
#     configs.set_config_by_name_key('database', 'use_web_api', True)
#     rx.Observable.interval(interval).start_with(0).subscribe(
#         on_next=lambda t: cycle(), on_error=lambda e: print(e))
