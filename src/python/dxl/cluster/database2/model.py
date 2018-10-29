from sqlalchemy import Column, Integer, String, DateTime, Enum, Table, MetaData, ForeignKey
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import mapper
import enum
import attr
import datetime
import typing
from .db import DataBase


class TaskState(enum.Enum):
    Unknown = 0
    Created = 1
    Pending = 2
    Submitted = 3
    Running = 4
    Completed = 5
    Failed = 6


class Worker(Enum):
    NoAction = 0,
    Local = 1,
    MultiThreading = 2,
    MultiProcessing = 3,
    Slurm = 4


meta = MetaData()

tasks = Table('tasks', meta,
              Column('id', Integer, primary_key=True),
              Column('state', Enum(TaskState, name='state_enum', metadata=meta)),
              Column('create', DateTime(timezone=True)),
              Column('submit', DateTime(timezone=True)),
              Column('finish', DateTime(timezone=True)),
              Column('depends', postgresql.ARRAY(Integer, dimensions=1)))


taskSlurm = Table(
    'taskSlurm', meta,
    Column('id', Integer, primary_key=True),
    Column('task_id', Integer, ForeignKey("tasks.id")),
    Column('worker', String),
    Column('workdir', String),
    Column('script', String)
)


taskSimu = Table(
    'taskSimu', meta,
    Column('id', Integer, primary_key=True),
    Column('taskSlurm_id', Integer, ForeignKey("taskSlurm.id"))
)


@attr.s(auto_attribs=True)
class Task:
    id: typing.Optional[int] = None
    scheduler: typing.Optional[str] = None
    state: TaskState = TaskState.Created
    create: typing.Optional[datetime.datetime] = None
    submit: typing.Optional[datetime.datetime] = None
    finish: typing.Optional[datetime.datetime] = None
    depends: typing.List[int] = ()


@attr.s(auto_attribs=True)
class TaskSlurm:
    id: typing.Optional[int] = None
    task_id: typing.Optional[int] = None
    worker: typing.Optional[str] = None
    workdir: typing.Optional[str] = None
    script: typing.Optional[str] = None


@attr.s(auto_attribs=True)
class TaskSimu:
    id: typing.Optional[int] = None
    taskSlurm_id: typing.Optional[int] = None


mapper(Task, tasks)
mapper(TaskSlurm, taskSlurm)
mapper(TaskSimu, taskSimu)


def create_all(database: DataBase):
    return meta.create_all(database.get_or_create_engine())


def drop_all(database: DataBase):
    return meta.drop_all(database.get_or_create_engine())
