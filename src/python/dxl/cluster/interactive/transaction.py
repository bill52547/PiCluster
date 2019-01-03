from ..database.model import TaskState, Task, SlurmTask, Mastertask
from ..database.db import DataBase
from ..dispatcher import methodispatch

import arrow


class TaskTransactions:
    def __init__(self, db: DataBase):
        self.db = db

    @methodispatch
    def post(self, t):
        return self.post(t)

    @methodispatch
    def read(self, t):
        return self.read(t)


@TaskTransactions.post.register(Task)
def _(self, t: Task):
    with self.db.session() as sess:
        sess.add(t)
        t.state = TaskState.Created
        t.create = arrow.utcnow().datetime
        sess.commit()
        return self.read(t)


@TaskTransactions.post.register(SlurmTask)
def _(self, t: SlurmTask, task_id: int):
    with self.db.session() as sess:
        sess.add(t)
        t.task_id = task_id
        sess.commit()
        return self.read(t)


@TaskTransactions.post.register(Mastertask)
def _(self, t: Mastertask):
    with self.db.session() as sess:
        sess.add(t)
        sess.commit()
        return self.read(t)


@TaskTransactions.read.register(Task)
def _(self, t: Task):
    with self.db.session() as sess:
        return sess.query(Task).get(t.id)


@TaskTransactions.read.register(SlurmTask)
def _(self, t: SlurmTask):
    with self.db.session() as sess:
        return sess.query(SlurmTask).get(t.id)


@TaskTransactions.read.register(Mastertask)
def _(self, t: Mastertask):
    with self.db.session() as sess:
        return sess.query(Mastertask).get((t.backend, t.id))
