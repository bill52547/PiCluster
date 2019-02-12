import typing
from typing import Callable, Generic
import attr
import subprocess
import multiprocessing
from functools import partial
import rx
from rx import Observable
from rx import operators as ops
from rx.concurrency import ThreadPoolScheduler

from ..interactive.web import Request


optimal_thread_count = multiprocessing.cpu_count()
pool_scheduler = ThreadPoolScheduler(optimal_thread_count)


T = typing.TypeVar("T")


def func(fn: Callable) -> Observable:
    """
    Turn a function to Observable.
    """
    return rx.create(fn).pipe(ops.subscribe_on(pool_scheduler))


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


def cli(body: str) -> func:
    def cmd(observer, scheduler, body=body):
        try:
            result = subprocess.run(body.split(" "), check=True, stdout=subprocess.PIPE).stdout.decode().split('\n')
            del result[-1]
            observer.on_next(result)
            observer.on_complete()
        except FileNotFoundError as err:
            observer.on_error(f"CMD error! {err}")
    return func(partial(cmd, body=body))


def query(resource: Resource) -> func:
    """
    Parse a url to a real resource, e.g. a Path, via query to database
    """
    return Query(resource)().pipe(ops.flat_map(lambda x: x))


# def create(item: T, table_name: str) -> Resource:
#     "create a record in database for one item, e.g. a File"
#     return Resource(table_name='ioCollections', primary_key=1)


def submit(task: func, backend: "Scheduler"):
    """
    Submit a **Task** to a scheduler, in the future, we may directly extend rx.Scheduler to fit our use.
    thus, currently, we need use submit(a_task, Slurm('192.168.1.131')).subscribe()
    in the future, we may use a_task.observe_on(Slurm('192.168.1.131')).subscribe() or a_task.subscribe_on(Slurm('ip'))
    """
    pass

