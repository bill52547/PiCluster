import rx
import requests
from rx import Observer

# from ..taskgraph.base import Graph
from ..interactive import web, base
from ..backend import srf, slurm
# from ..submanager.base import resubmit_failure

from apscheduler.schedulers.blocking import BlockingScheduler
from ..submanager.base import is_completed, is_failed
from ..backend.resource import allocate_node

from dxl.cluster.database2.api.tasks import taskSchema
from dxl.cluster.database2.model import Task, TaskState
from dxl.cluster.backend.slurm import sbatch, scontrol
from ..interactive.web import Request
from typing import List
import arrow


scheduler = rx.concurrency.ThreadPoolScheduler()
# _GET_API = "http://0.0.0.0:23300/api/v1/tasks"


class CycleService:
    @classmethod
    def cycle(cls):
        # print("*****************taskReset*****************")
        # task_reset()

        print("*****************Create2Pending****************")
        create2pending()
        print("********************runtask********************")
        run_task()
        print("*******************on_running******************")
        on_running()
        print("******************on_complete******************")
        on_complete()


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
        if task is not None:
            print(f"Task {task.id} is ready to go pending.")
            Request.patch(task.id, {"state": TaskState.Pending.value})

    def is_runnable(task):

        def do(c):
            print(__file__, c)
            if c == 0:
                # return rx.Observable.from_([(True, task)])
                return (True, task)
            else:
                return (False, None)
                # return rx.Observable.from_([(False, None)])

        if len(task.depends) == 0:
            return rx.Observable.from_([(True, task)])
        else:
            return Request.number_of_pending_depends(task.depends).map(do)
            # print("True returns")
            # return True
        # else:
        #     return rx.Observable.from_([(False, None)])

    class _Observer(Observer):
        def on_next(self, task):
            to_pending(task)

        def on_error(self, e):
            print("Got error: %s" % e)

        def on_completed(self):
            print("create2pending Sequence completed")
            print()

    # def debug_(x):
    #     print(x)
    #     return x

    (Request.read_state(TaskState.Created.value)
     .subscribe_on(scheduler)
     .flat_map(is_runnable)
     # .map(debug_)
     # .filter(lambda task: is_runnable(task))
     .filter(lambda x: x[0])
     .map(lambda x: x[1])
     .subscribe(_Observer()))


def run_task():
    """
    Get and launch tasks at pending state.
    """
    def to_submitted(taskSlurm):
        try:
            print(f"Submitting task: {taskSlurm.task_id}")
            Request.patch(taskSlurm.task_id, {"state": TaskState.Submitted.value,
                                              "submit": str(arrow.utcnow().datetime)})

        except Exception as e:
            print(e)

    def to_slurm(taskSlurm):
        if taskSlurm.workdir is not None and taskSlurm.script is not None:
            Request.taskSlurm_patch(taskSlurm.id,
                                    {"slurm_id": sbatch(workdir=taskSlurm.workdir, filename=taskSlurm.script)})
        return taskSlurm

    class _Observer(Observer):
        def on_next(self, taskSlurm):
            to_submitted(taskSlurm)

        def on_error(self, e):
            print("Got error: %s" % e)

        def on_completed(self):
            print("runtask Sequence completed")
            print()

    (Request.read_state(TaskState.Pending.value)
     .flat_map(lambda task: Request.cross_query(task))
     .map(lambda taskSlurm: to_slurm(taskSlurm))
     .subscribe(_Observer()))


def on_running():
    def to_running(taskSlurm):
        """
        scontrol returns: {"job_state":"COMPLETED"}
        :param taskSlurm:
        :return:
        """
        # TODO: Is it necessary to make scontrol streaming?
        # taskSlurm_state = scontrol(taskSlurm.slurm_id)["job_state"]

        # if taskSlurm_state == "RUNNING":
        if taskSlurm is not None:
            print(f"TaskSlurm: {taskSlurm.id} or {taskSlurm.task_id} is running ")
            Request.taskSlurm_patch(taskSlurm.id,
                                    {"slurm_state": TaskState.Running.name})

            Request.patch(taskSlurm.task_id,
                          {"state": TaskState.Running.value})

    def is_running(taskSlurm):

        def do_running(state):
            if state == "RUNNING":
                return rx.Observable.from_([(True, taskSlurm)])
            else:
                return rx.Observable.from_([(False, None)])

        return scontrol(taskSlurm.slurm_id).flat_map(do_running)

    def debug_(x):
        print(x)
        return x

    (Request.read_state(TaskState.Submitted.value)
     .flat_map(lambda task: Request.cross_query(task))
     # .flat_map(lambda taskSlurm: scontrol(taskSlurm.slurm_id))
     .flat_map(lambda taskSlurm: is_running(taskSlurm))
     .map(debug_)
     .filter(lambda x: x[0])
     .map(lambda x: x[1])
     .subscribe(to_running))


def on_complete():
    def to_complete(taskSlurm):
        # taskSlurm_state = scontrol(taskSlurm.slurm_id)["job_state"]

        # try:
        if taskSlurm is not None:
            print(f"{taskSlurm.slurm_id}, or TaskSlurm {taskSlurm.id} is completing.")
            Request.taskSlurm_patch(taskSlurm.id,
                                    {"slurm_state": TaskState.Completed.name})

            Request.patch(taskSlurm.task_id,
                          {"state": TaskState.Completed.value,
                           "finish": str(arrow.utcnow().datetime)})
        # except Exception as e:
        #     print(e)

    def is_completed(taskSlurm):

        def do_completed(state):
            if state == "COMPLETED":
                return rx.Observable.from_([(True, taskSlurm)])
            else:
                return rx.Observable.from_([(False, None)])

        return scontrol(taskSlurm.slurm_id).flat_map(do_completed)

    (Request.read_state(TaskState.Running.value)
     .flat_map(lambda task: Request.cross_query(task))
     .flat_map(lambda taskSlurm: is_completed(taskSlurm))
     .filter(lambda x: x[0])
     .map(lambda x: x[1])
     .subscribe(to_complete))