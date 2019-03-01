import attr
import enum
import typing
from enum import auto
import marshmallow as ma

from ...backend.base import AutoName


class SlurmOp(AutoName):
    scontrol = auto()
    scancel = auto()
    squeue = auto()
    sbatch = auto()


class SlurmTaskState(enum.Enum):
    Canceled = "CA"
    Created = "PD"
    Running = "R"
    Suspend = "S"
    Completed = "CD"
    Failed = "F"
    TimeOut = "TO"
    NodeFault = "NF"


@attr.s(auto_attribs=True)
class SqueueRow:
    job_id: typing.Optional[int] = None
    partition: typing.Optional[str] = None
    name: typing.Optional[str] = None
    user: typing.Optional[str] = None
    status: typing.Optional[str] = None
    time: typing.Optional[str] = None
    nodes: typing.Optional[str] = None
    node_list: typing.Optional[str] = None

    def __eq__(self, other):
        if isinstance(other, type(self)):
            self.job_id == other.job_id
        else:
            raise TypeError

    def __hash__(self):
        return hash(self.job_id)


class SqueueRowSchema(ma.Schema):
    job_id = ma.fields.Integer(allow_none=False)
    partition = ma.fields.String(allow_none=True)
    name = ma.fields.String(allow_none=True)
    user = ma.fields.String(allow_none=True)
    status = ma.fields.String(allow_none=True)
    time = ma.fields.String(allow_none=True)
    nodes = ma.fields.String(allow_none=True)
    node_list = ma.fields.String(allow_none=True)


squeueRowSchema = SqueueRowSchema()