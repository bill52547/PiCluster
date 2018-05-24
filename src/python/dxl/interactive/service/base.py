import json

import rx

from dxpy.time.utils import strf, strp

#from ..exceptions import InvalidJSONForTask, TaskDatabaseConnectionError, TaskNotFoundError
from ..base import DBprocess


class Service(DBprocess):
    def create_new(cls, task_json: str) -> int:
        """
        Create a task record in database.
        """
        task_db = json2db(task_json)
        return DBprocess.create(task_db)

    def read_all(cls, filter_func=None) -> 'rx.Observable<json str>':
        """
        for better understand
        """
        """
        Returns JSON serilized query results;

        Returns:
        - `str`: JSON loadable observalbe

        Raises:
        - None
        """
        if filter_func is None:
            def filter_func(x): return True
        return (rx.Observable
                .from_(cls.get_or_create_session().query(TaskDB).all())
                .filter(filter_func)
                .map(db2json))

    def json2db_update(cls, s):
        check_json(s, is_with_id=True)
        dct = json.loads(s)
        taskdb = cls.read_task(dct['id'])
        taskdb.desc = dct['desc']
        taskdb.data = json.dumps(dct['data'])
        taskdb.state = dct['state']
        taskdb.worker = dct['worker']
        taskdb.workdir = dct['workdir']
        taskdb.ttype = dct['type']
        taskdb.dependency = json.dumps(dct['dependency'])
        taskdb.time_create = strp(dct['time_stamp']['create'])
        taskdb.time_start = strp(dct['time_stamp']['start'])
        taskdb.time_end = strp(dct['time_stamp']['end'])
        taskdb.is_root = dct['is_root']
        return taskdb

    def update(cls, task_json):
        cls.json2db_update(task_json)
        cls.get_or_create_session().commit()
