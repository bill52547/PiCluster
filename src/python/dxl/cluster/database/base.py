import json
from .model import Database, TaskDB
from dxl.cluster.time.utils import strf, strp
import rx
from .exceptions import TaskNotFoundError,InvalidJSONForTask

class DBprocess:
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
    def db2json(cls,task):
        return json.dumps({
            '__task__': True,
            'id': task.id,
            'desc': task.desc,
            'data': json.loads(task.data),
            'worker': task.worker,
            'type': task.ttype, 
            'workdir': task.workdir,
            'dependency': json.loads(task.dependency),
            'father': json.loads(task.father),
            'time_stamp': {
                'create': strf(task.time_create),
                'start': strf(task.time_start),
                'end': strf(task.time_end)},
            'state': task.state,
            'is_root': task.is_root,
            'script_file': json.loads(task.script_file),
            'info': json.loads(task.info)
        })

    @classmethod
    def json2db(cls, s):
        check_json(s)
        dct = json.loads(s)
        return TaskDB(desc=dct['desc'],
                  data=json.dumps(dct['data']),
                  state=dct['state'],
                  workdir=dct['workdir'],
                  worker=dct['worker'],
                  ttype=dct['type'],
                  dependency=json.dumps(dct['dependency']),
                  father=json.dumps(dct['father']),
                  time_create=strp(dct['time_stamp']['create']),
                  is_root=dct['is_root'],
                  script_file=json.dumps(dct['script_file']),
                  info =json.dumps(dct['info']))

    @classmethod
    def create(cls, task_json: str) -> int:
        """
        Create a task record in database.
        """
        task_db = cls.json2db(task_json)
        cls.get_or_create_session().add(task_db)
        cls.get_or_create_session().commit()
        task_db = cls.get_or_create_session().query(TaskDB).get(task_db.id)
        return task_db.id
    
    @classmethod
    def read_taskdb(cls, tid):
        task_db = cls.get_or_create_session().query(TaskDB).get(tid)
        if task_db is None:
            raise TaskNotFoundError(tid)
        else:
            return task_db
    
    @classmethod
    def read(cls,tid):
        return cls.db2json(cls.read_taskdb(tid))
    
    @classmethod
    def read_all(cls, filter_func=None) -> 'rx.Observable<json str>':
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
                .map(cls.db2json))

    @classmethod
    def delete(cls, tid):    
        t = cls.read_taskdb(tid)
        if t is not None:
            cls.get_or_create_session().delete(t)
            cls.get_or_create_session().commit()

    @classmethod
    def update(cls, task_json):
        cls.json2db_update(task_json)
        cls.get_or_create_session().commit()

    @classmethod
    def json2db_update(cls, s):
        check_json(s,is_with_id=True)
        dct = json.loads(s)
        taskdb = cls.read_taskdb(dct['id'])
        taskdb.desc = dct['desc']
        taskdb.data = json.dumps(dct['data'])
        taskdb.state = dct['state']
        taskdb.worker = dct['worker']
        taskdb.workdir = dct['workdir']
        taskdb.ttype = dct['type']
        taskdb.dependency = json.dumps(dct['dependency'])
        taskdb.father = json.dumps(dct['father'])
        taskdb.time_create = strp(dct['time_stamp']['create'])
        if dct['time_stamp']['start'] != 'None':
            taskdb.time_start = strp(dct['time_stamp']['start'])
        if dct['time_stamp']['end'] != 'None':
            taskdb.time_end = strp(dct['time_stamp']['end'])
        taskdb.is_root = dct['is_root']
        taskdb.script_file = json.dumps(dct['script_file'])
        taskdb.info = json.dumps(dct['info'])
        return taskdb

def check_json(s, is_with_id=False):
    dct = json.loads(s)
    if not dct.get('__task__'):
        raise InvalidJSONForTask(
            "'__task__' not found or is False for JSON: {}".format(s))

    def check_key(dct, key, value_type=None):
        if not key in dct:
            raise InvalidJSONForTask(
                "Required key: {key} is not found in JSON string: {s}".format(key=key, s=s))
        if value_type is not None:
            if not isinstance(dct[key], value_type):
                raise InvalidJSONForTask(
                    "Wrong type for key: {k} with value: {v}".format(k=key, v=dct[key]))

    if is_with_id:
        check_key(dct, 'id', int)
    check_key(dct, 'desc', str)
    check_key(dct, 'data', dict)
    check_key(dct, 'time_stamp', dict)
    check_key(dct['time_stamp'], 'create', (str, type(None)))
    check_key(dct['time_stamp'], 'start', (str, type(None)))
    check_key(dct['time_stamp'], 'end', (str, type(None)))
    check_key(dct, 'state', str)
    check_key(dct, 'is_root', bool)
    check_key(dct, 'worker', str)
    check_key(dct, 'type', str)
    check_key(dct, 'workdir', str)
    check_key(dct, 'dependency', list)
    check_key(dct, 'father', list)
    check_key(dct, 'script_file',list)
    check_key(dct, 'info',dict)


    