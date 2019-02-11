import typing
from typing import Callable, Union, cast
# from dataclasses import dataclass
import subprocess
import attr
from typing import Generic, List
import rx

from rx.core import Observable, Observer
from ..interactive.web import Request


T = typing.TypeVar("T")


@attr.s(auto_attribs=True)
class Resource(Generic[T]):  # TODO: carefully define normal object and Re
    """
    A resource which is queryable from database
    """

    table_name: str
    primary_key: int


class Requests(Request):
    def __init__(self, body, url=None):
        if url is not None:
            self.url = url
        else:
            self.url = super().url()
        self.body = body

    def url(self):
        return self.url


# @attr.s(auto_attribs=True)
class Task(Observable):
    """
    A Task which composed with inputs, outputs and observer on outputs.

    inputs: List[Observable[Resource]]
    output: Observable[Resource[T]]
    """

    def __init__(self, inputs, output):
        self.inputs = inputs
        self.output = output

        super().__init__()

    def subscribe(self,  # pylint: disable=too-many-arguments,arguments-differ
                  observer=None,
                  on_error=None,
                  on_completed=None,
                  on_next=None,
                  *,
                  scheduler=None):

        if observer:
            if hasattr(observer, "on_next"):
                pass
            else:
                on_next = observer

        return self.subscribe_(on_next, on_error, on_completed, scheduler)


# class Task(Generic[T], Observable[T]):
#
#     inputs: List[Observable[Resource]]
#     output: Observable[Resource[T]]
#
#     def subscribe(self, observer: Observer[T]):
#         pass


class Table(Generic[T]):
    name: str


def func(fn: Callable) -> Task:
    """
    Turn a function to task.

    :param fn: Callable[[], T]
    :return: Task[T]
    """
    return Observable.defer(lambda: Observable.start(fn))


def cli(command: str) -> Task:
    def cmd():
        result = subprocess.run(command.split(" "), check=True, stdout=subprocess.PIPE)
        return result.stdout.decode().split('\n')
    # return func(cmd)
    return Task(inputs=command, output=func(cmd))


def request(requests: Requests) -> Task:
    return Observable.from_(Request.read(table_name=requests.body.table_name,
                                         select='id',
                                         condition=requests.body.primary_key,
                                         returns=['url']))

#
#
# def query(resource: Resource[T]) -> Task[T]:
#     """
#     Parse a url to a real resource, e.g. a Path, via query to database
#     """
#     pass
#
#
# def create(item: T) -> Resource[T]:
#     "create a record in database for one item, e.g. a File"
#     pass
#
#
# def submit(task: Task[T], backend: Scheduler):
#     """
#     Submit a **Task** to a scheduler, in the future, we may directly extend rx.Scheduler to fit our use.
#     thus, currently, we need use submit(a_task, Slurm('192.168.1.131')).subscribe()
#     in the future, we may use a_task.observe_on(Slurm('192.168.1.131')).subscribe() or a_task.subscribe_on(Slurm('ip'))
#     """
#     pass

