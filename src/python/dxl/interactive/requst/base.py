import rx
import json

from ..model import Database, TaskDB
from ..base import DBprocess


class request(DBprocess):
    session = None

    @classmethod
    def create_session(cls):
        cls.session = Database.session()

    @classmethod
    def get_or_create_session(cls, path=None):
        if cls.session is None:
            cls.create_session()
        return cls.session

    @classmethod
    def clear_session(cls):
        if cls.session is not None:
            cls.session.close()
        cls.session = None

    @classmethod
    def commit(cls,tid):
        task_db = cls.get_or_create_session().query(TaskDB).get(tid)
        return task_db


    @classmethod
    def checkstatus(cls,db):
        pass

    @classmethod
    def checkgraph(cls):
        pass



    @classmethod
    def autosubmit(cls):
        pass
        

    def read_all() -> 'Observable<TaskPy>':
    return (database.read_all()
            .map(parse_json))

    def dependencies(task) -> 'Observable<TaskPy>':
    return rx.Observable.from_(task.dependency).map(read)


    def update(task) -> None:
        database.update(task.to_json())
        return task


    def mark_submit(task) -> None:
        return update(ts.submit(task))


    def mark_start(task) -> None:
        if task.time_stamp.start is None:
            task.time_stamp = TaskStamp(create=task.time_stamp.create,
                                    start=now(),
                                    end=task.time_stamp.end)
        return update(ts.start(task))


    def mark_complete(task) -> None:
        if task.time_stamp.end is None:
            task.time_stamp = TaskStamp(create=task.time_stamp.create,
                                    start=task.time_stamp.start,
                                    end=now())
        return update(ts.complete(task))