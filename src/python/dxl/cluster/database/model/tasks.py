from sqlalchemy import Column, Integer, String, DateTime, Enum, Table, MetaData, ForeignKey, Boolean
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import mapper
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
    Column('backend', String),
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
    Column('script', String)
)

masterTask = Table(
    'masterTask', meta,
    Column('id', Integer, primary_key=True),
    Column('slurmTask_id', Integer, ForeignKey("slurmTask.id"))
)

backends = Table(
    'backends', meta,
    Column('backend', String, primary_key=True)
)
# backends.insert().value(backend='slurm')


fileCollections = Table(
    'fileCollections', meta,
    Column('id', Integer, primary_key=True),
    Column('mac', String, ForeignKey("mac.url")),
    Column('post_script', Boolean),
    Column('run_script', Boolean),
    Column('material_db', Boolean),
    Column('verbose_mac', Boolean),
    Column('phantom_bin', Boolean),
    Column('header_phantom', Boolean),
    Column('activity_range', Boolean),
    Column('range_material', Boolean),
    Column('err', Boolean),
    Column('out', Boolean),
    Column('root', Boolean)
)

# ioCollections = Table(
#     'ioCollections', meta,
#     Column()
# )


macs = Table(
    'mac', meta,
    Column('id', Integer, primary_key=True),
    Column('comment', String),
    Column('url', String)
)


procedures = Table(
    'procedures', meta,
    Column('procedure', String, primary_key=True)
)

# procedureCollections = Table(
#     'procedureCollections', meta,
#     Column('id', Integer, primary_key=True),
#     Column('subdir_init', Boolean),
#     Column('bcast', Boolean),
#     Column('run_pygate_submit', Boolean),
#     Column('run_slurm', Boolean),
#     Column('run_gate', Boolean),
#     Column('run_root_to_listmode_bin', Boolean),
#     Column('run_listmode_bin_to_listmode_h5', Boolean),
#     Column('run_stir', Boolean),
#     Column('run_listmode_to_lor', Boolean),
#     Column('run_lor_to_sinogram', Boolean),
#     Column('run_sinogram_to_lor', Boolean),
#     Column('run_lor_recon', Boolean),
#     Column('run_srf_recon', Boolean)
# )


taskOPs = Table(
    'taskOPs', meta,
    Column('taskOP', String, primary_key=True),
    Column('backend', String, ForeignKey("backends.backend")),
    Column('procedure', Integer, ForeignKey("procedures.procedure")),
    Column('inputs', Integer, postgresql.ARRAY(Integer, dimensions=1)),
    Column('outputs', Integer, ForeignKey("fileCollections.id")),
    Column('on_complete', Integer, postgresql.ARRAY(Integer, dimensions=1))
)


macs = Table(
    'macs', meta,
    Column('mac', String, primary_key=True),
    Column('comments', String),
    Column('url', String)
)


phantomBins = Table(
    'phantomBins', meta,
    Column('phantomBin', String, primary_key=True),
    Column('comments', String),
    Column('url', String )
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
    id: typing.Optional[int] = None
    taskSlurm_id: typing.Optional[int] = None


mapper(Task, tasks)
mapper(SlurmTask, slurmTask)
mapper(Mastertask, masterTask)


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
    thenext = ma.fields.Integer(allow_none=True)


taskSchema = TasksSchema()


class SlurmTaskSchema(ma.Schema):
    id = ma.fields.Integer(allow_none=True)
    task_id = ma.fields.Integer(allow_none=True)
    slurm_id = ma.fields.Integer(allow_none=True)
    slurm_state = ma.fields.String(allow_none=True)
    worker = ma.fields.String(allow_none=True)
    workdir = ma.fields.String(allow_none=True)
    script = ma.fields.String(allow_none=True)


slurmTaskSchema = SlurmTaskSchema()


class MasterTaskSchema(ma.Schema):
    id = ma.fields.Integer(allow_none=True)
    taskSlurm_id = ma.fields.Integer(allow_none=True)


masterTaskschema = MasterTaskSchema()


def create_all(database: DataBase):
    return meta.create_all(database.get_or_create_engine())


def drop_all(database: DataBase):
    return meta.drop_all(database.get_or_create_engine())
