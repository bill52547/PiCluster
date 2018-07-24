import rx
from ..taskgraph.base import Graph
from ..interactive import web,base
from ..backend import srf,slurm
from ..submanager.base import resubmit_failure
from apscheduler.schedulers.blocking import BlockingScheduler


class CycleService:
    @classmethod
    def cycle(cls):
        backend_cycle()
        graph_cycle()
        resubmit_cycle()

    @classmethod
    def start(cls,cycle_intervel=None):
        scheduler = BlockingScheduler()
        scheduler.add_job(cls.cycle,'interval',seconds=5)
        try:
            cls.cycle()
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass


def resubmit_cycle():
    """
    失败任务再提交
    """
    failed_tasks = (web.Request().read_all().filter(lambda t:t.is_fail).to_list()
          .subscribe_on(rx.concurrency.ThreadPoolScheduler())
          .to_blocking().first())
    for i in failed_tasks:
        slurm.Slurm().submit(i)


def backend_cycle():
    """
    后端查询任务状态
    """
    running_tasks = (web.Request().read_all().filter(lambda t:t.is_running or t.is_pending).to_list()
            .subscribe_on(rx.concurrency.ThreadPoolScheduler())
            .to_blocking().first())
    for i in running_tasks:
        slurm.Slurm().update(i)
        

def graph_cycle():
    """
    任务提交
    """
    g = Graph(get_nodes(),get_depens())
    g.mark_complete()
    runable_tasks = g.all_runable()
    tasks_to_submit = (web.Request().read_all().filter(lambda t:t.id in runable_tasks)
              .filter(lambda t:t.is_before_submit).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
    for i in tasks_to_submit:
        task_submit = slurm.Slurm().submit(i)


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

        
