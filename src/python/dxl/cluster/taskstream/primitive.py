import typing
from typing import Callable, Generic
import attr
import subprocess
import os
from pathlib import Path
from functools import partial, reduce
import rx
from rx import Observable
from rx import operators as ops

from ..interactive.web import Request
from ..config.graphql import GraphQLConfig
from ..backend.slurm.slurm import SlurmSjtu
from ..database.model.schema import Task, TaskState


T = typing.TypeVar("T")


def tap(fn):
    def result(x):
        fn(x)
        return x
    return ops.map(result)


def func(fn: Callable) -> Observable:
    """
    Turn a function to Observable.
    """
    return rx.create(fn)


@attr.s(auto_attribs=True)
class Resource(Generic[T]):  # TODO: carefully define normal object and Re
    """
    A resource which is queryable from database
    """
    id: int


class Query(Request):
    """
    Parse a url to a real resource, e.g. a Path, via query to database

    usage:
    Query.from_resource(Resource(table_name='ioCollections', primary_key=1)).subscribe(print)
    """
    def __init__(self, url=None):
        if url is not None:
            self.url = url
        else:
            self.url = super().url()

    def url(self):
        return self.url

    @classmethod
    def from_resource(cls, resource: Resource) -> func:
        # print(f"DEBUG Query.from_resource {resource}: {Request.read(table_name=resource.table_name, select='id',condition=str(resource.primary_key),returns=resource.returns)}")
        return rx.of(Request.read(table_name="resources",
                                  select='id',
                                  condition=str(resource.id),
                                  returns=["urls"]))


# class Table(Generic[T]):
#     name: str


def task(t: Task) -> func:
    def _task(observer, scheduler):
        try:
            observer.on_next(t)
            observer.on_completed()
        except Exception as e:
            observer.on_error(e)
    return rx.create(_task)


def cli(body: str) -> func:
    def cmd(observer, scheduler, body=body):
        try:
            result = subprocess.run(body.split(" "), check=True, stdout=subprocess.PIPE).stdout.decode().split('\n')
            observer.on_next(result[:-1])
            observer.on_completed()
        except FileNotFoundError as err:
            observer.on_error(f"CMD error! {err}")
    return func(partial(cmd, body=body))


def is_file(file: "URL") -> bool:
    def _is_file(observer, scheduler):
        try:
            f = Path(file)
            observer.on_next(f.is_file())
            observer.on_completed()
        except Exception as e:
            observer.on_error(f"{f} is not a url.")
    return rx.create(_is_file)


def create(item: T, table_name: str) -> Resource:
    """create a record in database for one item, e.g. a File"""
    if isinstance(item, dict):
        pass
    else:
        try:
            item = attr.asdict(item)
        except Exception as e:
            print(f"Type error, not supported item. {e}")

    result = Request.insert(table_name=table_name, inserts=attr.asdict(item))
    result_id = GraphQLConfig.insert_returning_parser(result)

    return Resource(table_name=table_name, primary_key=result_id)


def submit(task: Task, backend: "Scheduler"=SlurmSjtu, track_in_db=False) -> "Observable['output']":
    """
    Submit a **Task** to a scheduler, in the future, we may directly extend rx.Scheduler to fit our use.
    thus, currently, we need use submit(a_task, Slurm('192.168.1.131')).subscribe()
    in the future, we may use a_task.observe_on(Slurm('192.168.1.131')).subscribe() or a_task.subscribe_on(Slurm('ip'))
    """
    def _submit(obs, scheduler):
        id_on_backend = int(backend.submit(task))
        if id_on_backend is not None:
            t = attr.evolve(task,
                            id_on_backend=id_on_backend,
                            state=TaskState.Running,
                            state_on_backend=TaskState.Running)
            #TODO
            # insert_or_update_task(t)
            obs.on_next(t)
            obs.on_completed()
            return
        raise Exception

    def _on_running(task: Task):
        slurm_id = task.id_on_backend
        return SlurmSjtu.completed().pipe(ops.filter(lambda completed: slurm_id in completed),
                                          ops.first(),
                                          ops.map(lambda _: task))

    def _on_completed(task):
        return attr.evolve(task, state=TaskState.Completed, state_on_backend=TaskState.Completed)

    def _on_return(task):
        outputs = task.outputs
        work_dir = task.workdir
        # print(f"DEBUG in primitive submit, task_workdir: {work_dir}, task_id_onSlurm: {task.id_on_backend}, task_outputs: {outputs}")

        def exists(item):
            # print(f"DEBUG in primitive exists: item: {item}")
            item_path = Path(str(work_dir) + "/" + str(item))
            # print(f"DEBUG in primitive exists: item_path: {item_path}")
            if item_path.is_dir() or item_path.is_file():
                return True
            return False

        if len(outputs) > 0:
            # print(f"DEBUG in primitive submit: outputs: {outputs}")
            if reduce(lambda x, y: x or y, list(map(exists, outputs))):
                return list(map(lambda item: str(work_dir)+"/"+str(item), outputs))
            else:
                raise FileNotFoundError(f"One or more excepted outputs in {outputs} not found.")
        return []

    return rx.create(_submit).pipe(
            ops.flat_map(_on_running),
            ops.map(_on_completed),
            ops.map(_on_return)
        )


def _to_graphql_style_array(l: list) -> str:
    return "{" + ",".join(str(i) for i in l) + "}"


def _serilize(task: Task):
    result = dict()
    task_dict = attr.asdict(task)
    for k,v in task_dict.items():
        if v is None:
            continue
        if isinstance(v, list):
            result[k] = _to_graphql_style_array(v)
        else:
            result[k] = v
    return result


def is_duplicate_task(task: Task):
    def _is_duplicate_task():
        # duplicated_task = Request.read(table_name="tasks",
        #                                select="inputs",
        #                                condition=_to_graphql_style_array(task.inputs),
        #                                returns=["id"])
        duplicated_task = read_output_by_input(task.inputs)

        num_duplicated_tasks = len(duplicated_task)
        if num_duplicated_tasks:
            assert num_duplicated_tasks == 1
            duplicated_task_state = Request.read(table_name="tasks",
                                                 select="id",
                                                 condition=str(duplicated_task[0]),
                                                 returns=["state"])

            if TaskState.Completed.name in duplicated_task_state:
                print(f"""Task with resources id: {task.inputs} has been executed, will return output of previous task: {duplicated_task[0]}.""")
                return True
            else:
                return False
        else:
            return False
    return rx.from_callable(_is_duplicate_task)


def insert_or_update_task(task: Task):
    dup_task = read_output_by_input(task.inputs)
    if len(dup_task):
        assert len(dup_task) == 1
        task.id = dup_task[0]
        return update_task(task)
    else:
        return Request.insert(table_name='tasks', inserts=_serilize(task))['data']['insert_tasks']['returning'][0]['id']


def read_output_by_input(inputs):
    return Request.read(table_name="tasks", select="inputs", condition=_to_graphql_style_array(inputs), returns=["outputs"])


def update_task(task: Task):
    return Request.updates(table_name="tasks", id=task.id, patches=_serilize(task))





# def insert_or_read_taskdb(task: Task) -> Task:


    #     task_id = duplicated_task[0]

    #     return Request.read(table_name="tasks", select="id", condition=task_id, returns=["outputs"])
    # else:
    #     return Request.insert(table_name='tasks', inserts=_serilize(task))['data']['insert_tasks']['returning'][0]['id']