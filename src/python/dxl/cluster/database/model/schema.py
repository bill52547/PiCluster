from sqlalchemy import Column, Integer, String, DateTime, Enum, Table, MetaData, ForeignKey, PrimaryKeyConstraint, JSON
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import mapper
from rx.subjects import Subject
from rx import Observable
import marshmallow as ma
import enum
import attr
import datetime
import typing
from ..db import DataBase


class TaskState(enum.Enum):
    Unknown = 0
    Created = 1
    Pending = 2
    Submitted = 3
    Running = 4
    Completed = 5
    Failed = 6


meta = MetaData()

tasks = Table(
    'tasks', meta,
    Column('id', Integer, primary_key=True),
    Column('state', Enum(TaskState, name='state_enum', metadata=meta)),
    Column('create', DateTime(timezone=True)),
    Column('submit', DateTime(timezone=True)),
    Column('finish', DateTime(timezone=True)),
    Column('depends', postgresql.ARRAY(Integer, dimensions=1)),
    Column('backend', String, ForeignKey("backends.backend")),
    Column('thenext', Integer)
)

slurmTask = Table(
    'slurmTask', meta,
    Column('id', Integer, primary_key=True),
    Column('task_id', Integer, ForeignKey("tasks.id")),
    Column('slurm_id', Integer),
    Column('slurm_state', String),
    Column('worker', String),
    Column('workdir', String),
    Column('script', String, ForeignKey("ioCollections.file_name"))
)

masterTask = Table(
    'masterTask', meta,
    Column('backend', String, ForeignKey("backends.backend")),
    Column('id', Integer),
    Column('config', JSON),
    PrimaryKeyConstraint('backend', 'id', name='masterTask_pk')
)

backends = Table(
    'backends', meta,
    Column('backend', String, primary_key=True)
)


# procedures = Table(
#     'procedures', meta,
#     Column('procedure', String, primary_key=True)
# )


taskOPs = Table(
    'taskOPs', meta,
    Column('title', String),
    # Column('backend', String, ForeignKey("backends.backend")),
    # Column('procedure', String, ForeignKey("procedures.procedure")),
    # Column('on_complete', String, ForeignKey("taskOPs.taskOP"))
    Column("inputs", postgresql.ARRAY(String, dimensions=1)),
    Column('outputs', postgresql.ARRAY(String, dimensions=1)),
    PrimaryKeyConstraint('title', 'inputs', name='taskOP_pk')
)


ioCollections = Table(
    'ioCollections', meta,
    Column('file_name', String, primary_key=True),
    Column('comments', String),
    Column('url', String)
)


macs = Table(
    'macs', meta,
    Column('id', Integer, primary_key=True),
    Column('mac', String),
    Column('comments', String),
    Column('url', String)
)


phantomBins = Table(
    'phantomBins', meta,
    Column('id', Integer, primary_key=True),
    Column('phantomBin', String),
    Column('comments', String),
    Column('url', String)
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
    thenext: typing.Optional[int] = None


@attr.s(auto_attribs=True)
class SlurmTask:
    id: typing.Optional[int] = None
    task_id: typing.Optional[int] = None
    slurm_id: typing.Optional[int] = None
    slurm_state: typing.Optional[str] = None
    worker: typing.Optional[str] = None
    workdir: typing.Optional[str] = None
    script: typing.Optional[str] = None


@attr.s(auto_attribs=True)
class Mastertask:
    backend: typing.Optional[str] = None
    id: typing.Optional[str] = None
    config: typing.Dict = None

    def casts(self):
        return Subject.from_(self.config['init']['broadcast']['targets'])

    def source(self):
        return Subject.from_(self.config['init']['external']).map(lambda x: x['source'])

    def inputs(self):
        return Observable.merge(self.casts(), self.source())


@attr.s(auto_attribs=True)
class TaskOP:
    title: typing.Optional[str] = None
    inputs: typing.Optional[list] = None
    outputs: typing.Optional[list] = None


mapper(Task, tasks)
mapper(SlurmTask, slurmTask)
mapper(Mastertask, masterTask)
mapper(TaskOP, taskOPs)


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
    id = ma.fields.Integer(allow_none=False)
    # scheduler = ma.fields.Url(allow_none=True)
    state = TaskStateField(attribute="state")
    create = ma.fields.DateTime(allow_none=True)
    submit = ma.fields.DateTime(allow_none=True)
    finish = ma.fields.DateTime(allow_none=True)
    depends = ma.fields.List(ma.fields.Integer())
    thenext = ma.fields.Integer(allow_none=True)


taskSchema = TasksSchema()


class SlurmTaskSchema(ma.Schema):
    id = ma.fields.Integer(allow_none=False)
    task_id = ma.fields.Integer(allow_none=False)
    slurm_id = ma.fields.Integer(allow_none=True)
    slurm_state = ma.fields.String(allow_none=True)
    worker = ma.fields.String(allow_none=True)
    workdir = ma.fields.String(allow_none=True)
    script = ma.fields.String(allow_none=True)


slurmTaskSchema = SlurmTaskSchema()


class MasterTaskSchema(ma.Schema):
    backend = ma.fields.String(allow_none=False)
    id = ma.fields.Integer(allow_none=False)
    config = ma.fields.Dict(allow_none=True)


masterTaskschema = MasterTaskSchema()


def create_all(database: DataBase):
    return meta.create_all(database.get_or_create_engine())


def drop_all(database: DataBase):
    return meta.drop_all(database.get_or_create_engine())