import rx
import requests
from rx import Observer

from ..taskgraph.base import Graph
from ..interactive import web, base
from ..backend import srf, slurm
# from ..submanager.base import resubmit_failure

from apscheduler.schedulers.blocking import BlockingScheduler
from ..submanager.base import is_completed, is_failed
from ..backend.resource import allocate_node

from dxl.cluster.database2.api.tasks import taskSchema
from dxl.cluster.database2.model import Task, TaskState
from dxl.cluster.backend.slurm import sbatch
from ..interactive.web import Request
from typing import List

scheduler = rx.concurrency.ThreadPoolScheduler()
# _GET_API = "http://0.0.0.0:23300/api/v1/tasks"


class CycleService:
    @classmethod
    def cycle(cls):
        # graph_cycle()
        # backend_cycle()
        # resubmit_cycle()
        # TODO Create to Pending phase



        # TODO check depends & submit phase
        # print("*****************taskReset*****************")
        # task_reset()


        print("*****************Create2Pending*****************")
        create2pending()
        print("********************runtask********************")
        run_task()


    @classmethod
    def start(cls, cycle_intervel=None):
        scheduler = BlockingScheduler()
        scheduler.add_job(cls.cycle, 'interval', seconds=10)
        try:
            cls.cycle()
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass


def task_reset():
    """
    Reset all tasks state to Created.
    """

    def reset(task):
        print(f"Task {task.id} is reset.")
        Request.patch(task.id, {"state": TaskState.Created.value})

    class _Observer(Observer):
        def on_next(self, task):
            reset(task)

        def on_error(self, e):
            print("Got error: %s" % e)

        def on_completed(self):
            print("taskReset Sequence completed")
            print()

    Request.read_all().subscribe_on(scheduler).subscribe(_Observer())


def create2pending():
    """
    Checking depends and converting task state to pending if all depends are completed.

    :return: No return, but launch a stream.
    """

    def to_pending(task):
        """
        Converting task state to TaskState.Pending
        :param task: A task obj with TaskState specified in query method.
        :return: Task info in json.
        """
        print(f"Task {task.id} is ready to go pending.")
        Request.patch(task.id, {"state": TaskState.Pending.value})

    def is_runnable(task):
        if len(task.depends) == 0:
            return True
        elif Request.task_slurm_depends_checking(task.depends):
            return True
        else:
            return False

    class _Observer(Observer):
        def on_next(self, task):
            to_pending(task)

        def on_error(self, e):
            print("Got error: %s" % e)

        def on_completed(self):
            print("create2pending Sequence completed")
            print()

    (Request.read_state(TaskState.Created.value)
     .subscribe_on(scheduler)
     .filter(lambda task: is_runnable(task))
     .subscribe(_Observer()))


def run_task():
    """
    Get and launch tasks at pending state.
    """
    # def query(observer):
    #     result = requests.request("GET", _GET_API+"?state=2").json()
    #     for r in result:
    #         t = Task(**taskSchema.load(r))
    #         print(f"Task {t.id} to be submitted.")
    #         if t.state == TaskState.Pending:
    #             observer.on_next(t)
    #     observer.on_completed()

    def to_submitted(task_id):
        print(f"Submitting task: {task_id}")
        Request.patch(task_id, {"state": TaskState.Submitted.value})

        # requests.request("PATCH",
        #                  _GET_API+f"/{task_id}",
        #                  data={'state': TaskState.Submitted.value})
        # return task_id

    class _Observer(Observer):
        def on_next(self, task):
            to_submitted(task.id)

        def on_error(self, e):
            print("Got error: %s" % e)

        def on_completed(self):
            print("runtask Sequence completed")
            print()

    # (rx.Observable.create(query)
    #  .subscribe_on(scheduler)
    #  .flat_map(lambda t: TaskSleepBackend.submit(t.id))
    #  .map(to_submitted)
    #  .subscribe(_Observer()))
    (Request.read_state(TaskState.Pending.value)
     .subscribe_on(scheduler)
     .subscribe(_Observer()))


# 用slurm 查询任素状态，返回更新数据库
# def update_task_state():
#     def query(observer):
#         tasks = requests.request("GET", _GET_API).json()
#         for task in tasks:
#             t = Task(**taskSchema.load(r))
#             print(f"Updating state of task {t.id}.")
#             if t.state == TaskState.Running or t.state == TaskState.Pending:
#                 observer.on_next(t)
#         observer.on_completed()
#
#     def update_state(task_id):
#         pass















# def backend_cycle():
#     """
#     任务状态更新
#     """
#     running_tasks = (web.Request().read_all()
#                         .filter(lambda t:t.is_running or t.is_pending)
#                         .to_list().to_blocking().first())
#
#     if running_tasks == []:
#         return running_tasks
#     else:
#         for i in running_tasks:
#             if i.worker==base.Worker.Slurm:
#                 new_i=slurm.Slurm().update(i)
#             else:
#                 if is_failed(i):
#                     new_i = i.update_state(base.State.Failed)
#                 else:
#                     if is_completed(i):
#                         new_i = i.update_state(base.State.Complete)
#                     else:
#                         new_i = i.update_state(base.State.Runing)
#             web.Request().update(new_i)
#
#
# def graph_cycle():
#     """
#     任务提交
#     """
#     g = Graph(get_nodes(),get_depens())
#     g.mark_complete()
#     runable_tasks = g.all_runable()
#     tasks_to_submit = (web.Request().read_all()
#               .filter(lambda t:t.id in runable_tasks)
#               .filter(lambda t:t.is_before_submit)
#               .to_list()
#               .subscribe_on(rx.concurrency.ThreadPoolScheduler())
#               .to_blocking().first())
#     for i in tasks_to_submit:
#         if i.is_depen_gpu:
#             ni= allocate_node(i)
#         else:
#             ni=i
#         if ni is not None:
#             if ni.worker==base.Worker.Slurm:
#                 task_submit = slurm.Slurm().submit(i)
#             elif ni.worker==base.Worker.NoAction:
#                 task_submit = ni.update_state(base.State.Runing)
#             web.Request().update(task_submit)
#
#
# def get_graph_task():
#     beforesubmit = (web.Request().read_all().filter(lambda t: t.is_before_submit or
#                                                               t.is_running or
#                                                               t.is_pending)
#               .to_list()
#               .subscribe_on(rx.concurrency.ThreadPoolScheduler())
#               .to_blocking().first())
#     return beforesubmit
#
#
# def get_nodes():
#     nodes = []
#     tasks = get_graph_task()
#     for i in tasks:
#         nodes.append(i.id)
#     return nodes
#
#
# def get_depens():
#     depens = []
#     tasks = get_graph_task()
#     for i in tasks:
#         depens.append(i.dependency)
#     return depens