import rx
from rx import Observer
from apscheduler.schedulers.blocking import BlockingScheduler
from dxl.cluster.database.model import Task, TaskState
from dxl.cluster.backend.slurm import sbatch, scontrol
from ..interactive.web import Request
import arrow


scheduler = rx.concurrency.ThreadPoolScheduler()


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
        Request.patch(task.id, {"state": TaskState.Created.name})

    class _Observer(Observer):
        def on_next(self, task):
            reset(task)

        def on_error(self, e):
            print("Got error: %s" % e)

        def on_completed(self):
            print("taskReset Sequence completed")
            print()

    Request.read_all().subscribe_on(scheduler).subscribe(_Observer())


def tasks_to_track(num_limit=3):
    """
    Select 3 (by defualt) task simu, and track all sub
    """
    return (Request.tasksimu_to_check(num_limit)
            .flat_map(lambda t: Request.taskSimu_depends(t))
            .map(lambda x: x[0]).to_list()
            .flat_map(lambda l: Request.read_from_list(l)))


def tasksimu_to_track():
    return (Request.read_taskSimu()
            .flat_map(lambda t: Request.tasksimu_id_cast(t.id))
            .to_list()
            .flat_map(Request.read_from_list))


def create2pending():
    """
    Checking depends and converting task state to pending if all depends are completed.
    """

    def to_pending(task):
        """
        Converting task state to TaskState.Pending
        :param task: A task obj with TaskState specified in query method.
        :return: Task info in json.
        """
        if task is not None:
            print(f"Task {task.id} is ready to go pending.")
            Request.patch(task.id, {"state": TaskState.Pending.name})

    def is_runnable(task):
        def do(c):
            if c == 0:
                return (True, task)
            else:
                return (False, None)

        if len(task.depends) == 0:
            return rx.Observable.from_([(True, task)])
        else:
            return Request.dependency_checking(task.id).map(do)

    class _Observer(Observer):
        def on_next(self, task):
            to_pending(task)

        def on_error(self, e):
            print("Got error in create2pending: %s" % e)

        def on_completed(self):
            print("create2pending sequence completed")
            print()

    (rx.Observable.merge(tasks_to_track(), tasksimu_to_track())
     .filter(lambda t: t.state == TaskState.Created)
     .subscribe_on(scheduler)
     .flat_map(lambda task: is_runnable(task))
     .filter(lambda x: x[0])
     .map(lambda x: x[1])
     .subscribe(_Observer()))


def run_task():

    def to_submitted(taskSlurm):
        try:
            print(f"Submitting task: {taskSlurm.task_id}")
            Request.patch(taskSlurm.task_id, {"state": TaskState.Submitted.name,
                                              "submit": str(arrow.utcnow().datetime)})

        except Exception as e:
            print(e)

    def to_slurm(taskSlurm):
        if taskSlurm.workdir is not None and taskSlurm.script is not None:
            Request.patch_taskSlurm(taskSlurm.id,
                                    {"slurm_id": sbatch(workdir=taskSlurm.workdir, filename=taskSlurm.script)})
        return taskSlurm

    class _Observer(Observer):
        def on_next(self, taskSlurm):
            to_submitted(taskSlurm)

        def on_error(self, e):
            print("Got error in run_task: %s" % e)

        def on_completed(self):
            print("runtask Sequence completed")
            print()

    (rx.Observable.merge(tasks_to_track(), tasksimu_to_track())
     .filter(lambda t: t.state == TaskState.Pending)
     .map(lambda t: t.id).to_list()
     .flat_map(lambda l: Request.task_to_taskslurm(l))
     .map(lambda taskSlurm: to_slurm(taskSlurm))
     .subscribe(_Observer()))


def on_running():
    def to_running(taskSlurm):
        """
        scontrol returns: {"job_state":"COMPLETED"}
        :param taskSlurm:
        :return:
        """
        if taskSlurm is not None:
            print(f"TaskSlurm: {taskSlurm.id} or {taskSlurm.task_id} is running ")
            Request.patch_taskSlurm(taskSlurm.id,
                                    {"slurm_state": TaskState.Running.name})

            Request.patch(taskSlurm.task_id,
                          {"state": TaskState.Running.name})

    def is_running(taskSlurm):

        def do_running(state):
            if state == "RUNNING":
                return rx.Observable.from_([(True, taskSlurm)])
            else:
                return rx.Observable.from_([(False, None)])

        return scontrol(taskSlurm.slurm_id).flat_map(do_running)

    (rx.Observable.merge(tasks_to_track(), tasksimu_to_track())
     .filter(lambda t: t.state == TaskState.Submitted)
     .map(lambda t: t.id).to_list()
     .flat_map(lambda l: Request.task_to_taskslurm(l))
     .flat_map(lambda taskSlurm: is_running(taskSlurm))
     .filter(lambda x: x[0])
     .map(lambda x: x[1])
     .subscribe(to_running))


def on_complete():
    def to_complete(taskSlurm):
        if taskSlurm is not None:
            print(f"{taskSlurm.slurm_id}, or TaskSlurm {taskSlurm.id} is completing.")
            Request.patch_taskSlurm(taskSlurm.id,
                                    {"slurm_state": TaskState.Completed.name})

            Request.patch(taskSlurm.task_id,
                          {"state": TaskState.Completed.name,
                           "finish": str(arrow.utcnow().datetime)})

    def is_completed(taskSlurm):

        def do_completed(state):
            if state == "COMPLETED":
                return rx.Observable.from_([(True, taskSlurm)])
            else:
                return rx.Observable.from_([(False, None)])

        return scontrol(taskSlurm.slurm_id).flat_map(do_completed)

    (rx.Observable.merge(tasks_to_track(), tasksimu_to_track())
     .filter(lambda t: t.state in [TaskState.Running, TaskState.Submitted])
     .map(lambda t: t.id).to_list()
     .flat_map(lambda l: Request.task_to_taskslurm(l))
     .flat_map(lambda taskSlurm: is_completed(taskSlurm))
     .filter(lambda x: x[0])
     .map(lambda x: x[1])
     .subscribe(to_complete))