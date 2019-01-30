import typing
# from dataclasses import dataclass
import subprocess
import attr
from typing import Generic, List
import rx

from rx import Observable, Observer


@attr.s(auto_attribs=True)
class Resource(Generic[T]):  # TODO: carefully define normal object and Re
    """
    A resource which is queryable from database
    """

    table_name: str
    primary_key: int


@attr.s(auto_attribs=True)
class Request(Generic[T]):
    url: str
    body: str


class Task(Generic[T], Observable[T]):
    inputs: List[Observable[Resource]]
    output: Observable[Resource[T]]

    def subscribe(self, observer: Observer[T]):
        pass


class Table(Generic[T]):
    name: str


def func(fn: typing.Callable[[], T]) -> Task[T]:
    return Observable.defer(lambda: Observable.start(fn))


def cli(command: str) -> Task[typing.List[str]]:
    def cmd():
        result = subprocess.run(command.split(" "), check=True, stdout=subprocess.PIPE)
        return result.stdout.decode().split('\n')
    return func(cmd)


def request(request: Request[T]) -> Task[T]:

    pass


def query(resource: Resource[T]) -> Task[T]:
    """
    Parse a url to a real resource, e.g. a Path, via query to database
    """
    pass


def create(item: T) -> Resource[T]:
    "create a record in database for one item, e.g. a File"
    pass


def submit(task: Task[T], backend: Scheduler):
    """
    Submit a **Task** to a scheduler, in the future, we may directly extend rx.Scheduler to fit our use.
    thus, currently, we need use submit(a_task, Slurm('192.168.1.131')).subscribe()
    in the future, we may use a_task.observe_on(Slurm('192.168.1.131')).subscribe() or a_task.subscribe_on(Slurm('ip'))
    """
    pass

