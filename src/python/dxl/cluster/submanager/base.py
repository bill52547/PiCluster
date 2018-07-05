from ..interactive import base,web
from ..interactive.base import Task
import rx 


def find_sub(task):
    return (web.Request().read_all().filter(lambda t:t.father==[task.id]))

def complete_rate(task):
    subs = find_sub(task)
    num_subs = len(subs.to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
    complete = (subs.filter(lambda t:t.state==base.State.Complete).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
    rate = len(complete)/num_subs
    return rate

def resubmit_failure(task:Task):
    subs = find_sub(task)
    failure = (subs.filter(lambda t:t.state == base.State.Failed).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
    for i in failure:
        old_id = i.id
        i.id = None
        tasknew = web.Request().create(i)
        tasks = subs.filter(lambda t:old_id in t.dependency).to_list().to_blocking().first()
        for j in range(0,len(tasks)):
            tasks[j].replace_dependency(old_id,tasknew.id)
            web.Request().update(tasks[j])
        
class Service:
    def cycle(self,task):
        scheduler = BlockingScheduler()
        scheduler.add_job(resubmit_failure(task),'interval',seconds=5)
        try:
            resubmit_failure(task)
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            pass

