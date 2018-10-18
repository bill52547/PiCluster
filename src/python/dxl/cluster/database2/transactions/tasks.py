from dxl.cluster.database2 import TaskState
from dxl.cluster.database2.model import Task
from dxl.cluster.database2.db import DataBase
from sqlalchemy import func
import attr


class TaskTransactions:
    def __init__(self, db: DataBase):
        self.db = db

    def create(self, t: Task):
        with self.db.session() as sess:
            sess.add(t)
            sess.commit()
            return self.read(t.id)

    def read(self, task_id: int):
        with self.db.session() as sess:
            return sess.query(Task).get(task_id)

    def read_all(self):
        with self.db.session() as sess:
            return sess.query(Task).all()

    def update(self, task_id: int, changes: dict):
        with self.db.session() as sess:
            to_update = sess.query(Task).get(task_id)
            attr.evolve(to_update, **changes)
            sess.commit()
            return self.read(task_id)

    def count_all(self):
        with self.db.session() as sess:
            return sess.query(Task).count()

    def filter(self, state):
        with self.db.session() as sess:
            return sess.query(Task).filter_by(state=state).all()

    def count_filter(self, state):
        with self.db.session() as sess:
            return sess.query(Task).filter_by(state=state).count()