from dxl.cluster.database import TaskState
from dxl.cluster.database.model import Task, TaskSlurm, TaskSimu
from dxl.cluster.database.db import DataBase
import arrow


class TaskTransactions:
    def __init__(self, db: DataBase):
        self.db = db

    def create(self, t: Task):
        """
        Create Task
        :param t:
        :return:
        """
        with self.db.session() as sess:
            sess.add(t)
            t.state = TaskState.Created
            t.create = arrow.utcnow().datetime
            sess.commit()
            return self.read(t.id)

    def create_taskSlurm(self, t: TaskSlurm, task_id: int):
        """
        Create TaskSlurm
        :param t:
        :param task_id:
        :return:
        """
        with self.db.session() as sess:
            sess.add(t)
            t.task_id = task_id
            sess.commit()
            return self.read_taskSlurm(t.id)

    def create_taskSimu(self, t: TaskSimu, taskSlurm_id: int):
        with self.db.session() as sess:
            sess.add(t)
            t.taskSlurm_id = taskSlurm_id
            sess.commit()
            return self.read_taskSimu(t.id)

    def read(self, task_id: int):
        """
        Read Task by Task.id
        :param task_id:
        :return:
        """
        with self.db.session() as sess:
            return sess.query(Task).get(task_id)

    def read_taskSlurm(self, task_id: int):
        """
        Read taskSlurm by TaskSlurm.id
        :param task_id:
        :return:
        """
        with self.db.session() as sess:
            return sess.query(TaskSlurm).get(task_id)

    def read_taskSimu(self, task_id: int):
        with self.db.session() as sess:
            return sess.query(TaskSimu).get(task_id)


    #
    # def read_taskSlurm_by_taskid(self, task_id: int):
    #     with self.db.session() as sess:
    #         return sess.query(TaskSlurm).join(Task).filter(Task.id == task_id).first()

    # def read_all(self):
    #     with self.db.session() as sess:
    #         return sess.query(Task).all()
    #
    # def update(self, task_id: int, changes: dict):
    #     with self.db.session() as sess:
    #         to_update = sess.query(Task).get(task_id)
    #         for k, v in changes.items():
    #             setattr(to_update, k, v)
    #         sess.commit()
    #         return self.read(task_id)
    #
    # def task_slurm_update(self, task_slurm_id: int, changes: dict):
    #     with self.db.session() as sess:
    #         to_update = sess.query(TaskSlurm).get(task_slurm_id)
    #         for k, v in changes.items():
    #             setattr(to_update, k, v)
    #         sess.commit()
    #         return self.read_taskSlurm(task_slurm_id)
    #
    # def count_all(self):
    #     with self.db.session() as sess:
    #         return sess.query(Task).count()
    #
    # def filter(self, state):
    #     with self.db.session() as sess:
    #         return sess.query(Task).filter_by(state=state).all()
    #
    # def count_filter(self, state):
    #     with self.db.session() as sess:
    #         return sess.query(Task).filter_by(state=state).count()