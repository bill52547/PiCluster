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

    table_name: str
    primary_key: int


class Query(Request):
    def __init__(self, body, url=None):
        if url is not None:
            self.url = url
        else:
            self.url = super().url()
        self.body = body

    def url(self):
        return self.url

    def __call__(self) -> func:
        return rx.of(Request.read(table_name=self.body.table_name,
                                  select='id',
                                  condition=self.body.primary_key,
                                  returns=['url']))


class Table(Generic[T]):
    name: str


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


def query(resource: Resource) -> func:
    """
    Parse a url to a real resource, e.g. a Path, via query to database
    """
    return Query(resource)().pipe(ops.flat_map(lambda x: x))


def create(item: T, table_name: str) -> Resource:
    "create a record in database for one item, e.g. a File"
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


def submit(task: Task, backend: "Scheduler"=SlurmSjtu) -> "Observable['output']":
    """
    Submit a **Task** to a scheduler, in the future, we may directly extend rx.Scheduler to fit our use.
    thus, currently, we need use submit(a_task, Slurm('192.168.1.131')).subscribe()
    in the future, we may use a_task.observe_on(Slurm('192.168.1.131')).subscribe() or a_task.subscribe_on(Slurm('ip'))
    """
    def _submit(obs, scheduler):
        id_on_backend = int(backend.submit(task))
        if id_on_backend is not None:
            obs.on_next(attr.evolve(task,
                                    id_on_backend=id_on_backend,
                                    state=TaskState.Running,
                                    state_on_backend=TaskState.Running))
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
        cwd = os.getcwd()

        def exists(item):
            item_path = Path(cwd + "/" + item)
            if item_path.is_dir() or item_path.is_file():
                return True
            return False

        if len(outputs) > 0:
            if reduce(lambda x, y: x or y, list(map(exists, outputs))):
                return list(map(lambda item: cwd+"/"+item, outputs))
            else:
                raise FileNotFoundError
        return []

    return rx.create(_submit).pipe(
            ops.flat_map(_on_running),
            ops.map(_on_completed),
            ops.map(_on_return)
        )
