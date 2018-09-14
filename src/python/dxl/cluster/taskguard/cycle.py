import rx
from ..taskgraph.base import Graph
from ..interactive import web,base
from ..backend import srf,slurm
from ..submanager.base import resubmit_failure
from apscheduler.schedulers.blocking import BlockingScheduler
from ..submanager.base import is_completed,is_failed
from ..backend.resource import allocate_node


class CycleService:
    @classmethod
    def cycle(cls):
        graph_cycle()
        backend_cycle()
        #resubmit_cycle()

    @classmethod
    def start(cls,cycle_intervel=None):
        scheduler = BlockingScheduler()
        scheduler.add_job(cls.cycle,'interval',seconds=10)
        try:
            cls.cycle()
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass


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
    beforesubmit = (web.Request().read_all().filter(lambda t: t.is_before_submit or t.is_running or t.is_pending)
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


