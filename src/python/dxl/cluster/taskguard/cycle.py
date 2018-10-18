import rx
from ..taskgraph.base import Graph
from ..interactive import web,base
from ..backend import srf,slurm
from ..submanager.base import resubmit_failure
from apscheduler.schedulers.blocking import BlockingScheduler
from ..submanager.base import is_completed,is_failed
from ..backend.resource import allocate_node

import requests

scheduler = rx.concurrency.ThreadPoolScheduler(4)



class CycleService:
    @classmethod
    def cycle(cls):
        # graph_cycle()
        # backend_cycle()
        #resubmit_cycle()
        # create2pending()
        runtask()

    @classmethod
    def start(cls,cycle_intervel=None):
        scheduler = BlockingScheduler()
        scheduler.add_job(cls.cycle,'interval',seconds=10)
        try:
            cls.cycle()
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass

_GET_API = "http://localhost:23300/api/v1/tasks"
from dxl.cluster.database2.api.tasks import schema
from dxl.cluster.database2.model import Task, TaskState
def create2pending():
    def query(observer):
        result = requests.request("GET", _GET_API+"?state=0").json()
        for r in result:
            t = Task(**schema.load(r))
            print(t)
            observer.on_next(t)
        observer.on_completed()

    def to_pending(task):
        # print(task)
        # return rx.Observable.create(lambda : requests.request("PATCH", _GET_API+f"/{task.id}", data={"state": TaskState.Pending.value}))
        proc = lambda : requests.request("PATCH", _GET_API+f"/{task.id}", data={"state": TaskState.Pending.value})
        return proc().json()


    tasks = (rx.Observable.create(query)
             .map(to_pending)
             .subscribe_on(scheduler)
             .subscribe(lambda t: print(t)))

from dxl.cluster.backend.test_sleep import TaskSleepBackend

def runtask():
    def to_complete(task_id):
        requests.request("PATCH", _GET_API+f"/{task_id}", data={'state': TaskState.Completed.value})
        return task_id

    def query(observer):
        result = requests.request("GET", _GET_API).json()
        for r in result:
            t = Task(**schema.load(r))
            if t.state == TaskState.Pending:
                observer.on_next(t)
        observer.on_completed()

    (rx.Observable.create(query)
     .flat_map(lambda t: TaskSleepBackend.submit(t.id))
     .map(to_complete)
     .subscribe(lambda task_id: print(task_id)))







def backend_cycle():
    """
    任务状态更新
    """
    running_tasks = (web.Request().read_all()
          .filter(lambda t:t.is_running or t.is_pending)
          .to_list().to_blocking().first())

    if running_tasks==[]:
        return running_tasks
    else:
        for i in running_tasks:
            if i.worker==base.Worker.Slurm:
                new_i=slurm.Slurm().update(i)
            else:
                if is_failed(i):
                    new_i = i.update_state(base.State.Failed)
                else:
                    if is_completed(i):
                        new_i = i.update_state(base.State.Complete)
                    else:
                        new_i = i.update_state(base.State.Runing)
            web.Request().update(new_i)


def graph_cycle():
    """
    任务提交
    """
    g = Graph(get_nodes(),get_depens())
    g.mark_complete()
    runable_tasks = g.all_runable()
    tasks_to_submit = (web.Request().read_all()
              .filter(lambda t:t.id in runable_tasks)
              .filter(lambda t:t.is_before_submit)
              .to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
    for i in tasks_to_submit:
        if i.is_depen_gpu:
            ni= allocate_node(i)
        else:
            ni=i
        if ni is not None:
            if ni.worker==base.Worker.Slurm:
                task_submit = slurm.Slurm().submit(i)
            elif ni.worker==base.Worker.NoAction:
                task_submit = ni.update_state(base.State.Runing)
            web.Request().update(task_submit)


def get_graph_task():
    beforesubmit = (web.Request().read_all().filter(lambda t: t.is_before_submit or
                                                              t.is_running or
                                                              t.is_pending)
              .to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
    return beforesubmit

def get_nodes():
    nodes = []
    tasks = get_graph_task()
    for i in tasks:
        nodes.append(i.id)
    return nodes

def get_depens():
    depens = []
    tasks = get_graph_task()
    for i in tasks:
        depens.append(i.dependency)
    return depens


