from ..interactive import base,web
from ..interactive.base import Task
import rx 


def find_sub(task):
    return (web.Request().read_all().filter(lambda t:t.father==[task.id]))

# def find_unroot():
#     return (web.Request().filter(lambda t:t.is_root == False))

# def find_sub(task):
#     return task.dependency

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
        


    
