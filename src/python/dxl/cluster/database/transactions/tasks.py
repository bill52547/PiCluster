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
