import time
import rx
scheduler = rx.concurrency.ThreadPoolScheduler(4)
class TaskSleepBackend:
    @classmethod
    def submit(cls, task_id):
        def dummpy_run(observer):
            time.sleep(3)
            observer.on_next(task_id)
            observer.on_completed()
        return rx.Observable.create(dummpy_run).subscribe_on(scheduler)