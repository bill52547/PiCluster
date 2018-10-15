from sqlalchemy import Column, Integer, String, DateTime, Enum, Table, MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import mapper
import enum
import attr
import datetime
import typing
from .db import DataBase

# TODO: move definition of Task to upper level, thus support from `dxl.cluster` import `Task`.

class TaskState(enum.Enum):
    Unknown = 0
    Created = 1
    Pending = 2
    Submitted = 3
    Running = 4
    Completed = 5
    Failed = 6


meta = MetaData()

tasks = Table('tasks', meta,
              Column('id', Integer, primary_key=True),
              Column('state', Enum(TaskState, name='state_enum', metadata=meta)),
              Column('create', DateTime(timezone=True)),
              Column('submit', DateTime(timezone=True)),
              Column('finish', DateTime(timezone=True)),
              Column('depens', postgresql.ARRAY(Integer, dimensions=1)))


@attr.s(auto_attribs=True)
class Task:
    id: typing.Optional[int] = None
    scheduler: typing.Optional[str] = None
    state: TaskState = TaskState.Unknown
    create: typing.Optional[datetime.datetime] = None
    submit: typing.Optional[datetime.datetime] = None
    finish: typing.Optional[datetime.datetime] = None
    depens: typing.Tuple[int] = ()


mapper(Task, tasks)


def create_all(database: DataBase):
    return meta.create_all(database.get_or_create_engine())


def drop_all(database: DataBase):
    return meta.drop_all(database.get_or_create_engine())
