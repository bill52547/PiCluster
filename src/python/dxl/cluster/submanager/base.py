from ..interactive import base,web
from ..interactive.base import Task
import rx 


def find_sub(task):
    return (web.Request().read_all().filter(lambda t:t.father==[task.id]))

def num_subs(task):
    return len(find_sub(task).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())


def is_completed(task):
    subs = find_sub(task)

    if num_subs(task) == 0:
        return 1
    else:
        complete = (subs.filter(lambda t:t.state==base.State.Complete).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
        return  len(complete)/num_subs(task)


def is_failed(task):
    subs = find_sub(task)

    if num_subs(task) == 0:
        return 0
    else:
        failure = (subs.filter(lambda t:t.state==base.State.Failed).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
        return len(failure)/num_subs(task)

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
        


    
