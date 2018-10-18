from dxl.cluster.database2 import TaskState
from dxl.cluster.database2.model import Task
from dxl.cluster.database2.db import DataBase
from sqlalchemy import func
import arrow
import attr


class TaskTransactions:
    def __init__(self, db: DataBase):
        self.db = db

    def create(self, t: Task):
        if t.state != TaskState.Unknown:
            raise ValueError(f"New tasks should have state {TaskState.Unknown}, got {t.state}.")
        with self.db.session() as sess:
            sess.add(t)
            t.state = TaskState.Created
            t.create = arrow.utcnow().datetime
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
            # if 'state' in changes and isinstance(changes['state'], int):
            #     changes['state'] = TaskState(changes['state'])
            # to_update = attr.evolve(to_update, **changes)
            for k, v in changes.items():
                setattr(to_update, k, v)
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