# # import time
# # import rx
# # # scheduler = rx.concurrency.ThreadPoolScheduler(4)
# # # class TaskSleepBackend:
# # #     @classmethod
# # #     def submit(cls, task_id):
# # #         def dummpy_run(observer):
# # #             time.sleep(3)
# # #             observer.on_next(task_id)
# # #             observer.on_completed()
# # #         return rx.Observable.create(dummpy_run).subscribe_on(scheduler)
#
# from ..database.model.schema import Task, TaskState
# import arrow
#
#
# sleep_10 = Task(id=1, state=TaskState.Created,
#                 create=arrow.now(), submit=None, finish=None,
#                 depends=[], scheduler=None, backend='slurm',
#                 workdir='/mnt/gluster/qinglong/tasks.dev/',
#                 id_on_backend=None,
#                 state_on_backend=None,
#                 worker=None,
#                 script='run_sleep.sh',
#                 fn='monte-carlo-simulation@1.0.0')
