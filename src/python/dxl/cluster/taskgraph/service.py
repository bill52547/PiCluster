import rx
from .base import Graph
from ..interactive import base,web
from apscheduler.schedulers.blocking import BlockingScheduler


class DemonService:
    def cycle(self):
        g = Graph(self.get_nodes(),self.get_depens())
        g.mark_complete()
        tasks = g.all_runable()
        tasks_to_submit = (web.Request().read_all().filter(lambda t:t.id in tasks)
              .filter(lambda t:t.is_before_submit).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
        for i in tasks_to_submit:
            task_submit = web.submit(i)

    def start(self):
        scheduler = BlockingScheduler()
        scheduler.add_job(self.cycle,'interval',seconds=5)
        try:
            self.cycle()
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass

    def get_graph_task(self):
        beforesubmit = (web.Request().read_all().filter(lambda t: t.is_before_submit or t.is_pending or t.is_running)
              .to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
        return beforesubmit

    def get_nodes(self):
        nodes = []
        tasks = self.get_graph_task()
        for i in tasks:
            nodes.append(i.id)
        return nodes

    def get_depens(self):
        depens = []
        tasks = self.get_graph_task()
        for i in tasks:
            depens.append(i.dependency)
        return depens
