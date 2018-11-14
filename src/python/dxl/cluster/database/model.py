from sqlalchemy import Column, Integer, String, DateTime, Enum, Table, MetaData, ForeignKey
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import mapper
import marshmallow as ma
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


# TODO tobe removed
# class Worker(Enum):
#     NoAction = 0,
#     Local = 1,
#     MultiThreading = 2,
#     MultiProcessing = 3,
#     Slurm = 4


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
    Column('slurm_id', Integer),
    Column('slurm_state', String),
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
    slurm_id: typing.Optional[int] = None
    slurm_state: typing.Optional[str] = None
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


class TaskStateField(ma.fields.Field):
    """
    Serialization/deserialization utils
    """

    def _serialize(self, value, attr, obj):
        if value is None:
            return ''
        return value.name

    def _deserialize(self, value, attr, data):
        if isinstance(value, int):
            value = int(value)
            return TaskState(value)
        elif isinstance(value, str):
            return TaskState[value]


class TasksSchema(ma.Schema):
    id = ma.fields.Integer(allow_none=True)
    # scheduler = ma.fields.Url(allow_none=True)
    state = TaskStateField(attribute="state")
    create = ma.fields.DateTime(allow_none=True)
    submit = ma.fields.DateTime(allow_none=True)
    finish = ma.fields.DateTime(allow_none=True)
    depends = ma.fields.List(ma.fields.Integer())


taskSchema = TasksSchema()


class TaskSlurmSchema(ma.Schema):
    id = ma.fields.Integer(allow_none=True)
    task_id = ma.fields.Integer(allow_none=True)
    slurm_id = ma.fields.Integer(allow_none=True)
    slurm_state = ma.fields.String(allow_none=True)
    worker = ma.fields.String(allow_none=True)
    workdir = ma.fields.String(allow_none=True)
    script = ma.fields.String(allow_none=True)


taskSlurmSchema = TaskSlurmSchema()


class TaskSimuSchema(ma.Schema):
    id = ma.fields.Integer(allow_none=True)
    taskSlurm_id = ma.fields.Integer(allow_none=True)


taskSimuSchema = TaskSimuSchema()


def create_all(database: DataBase):
    return meta.create_all(database.get_or_create_engine())


def drop_all(database: DataBase):
    return meta.drop_all(database.get_or_create_engine())
