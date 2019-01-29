from dxl.cluster.database.model import Task, Mastertask, taskSchema, masterTaskschema
from dxl.cluster.backend.slurm.schema import SqueueRow, squeueRowSchema

from functools import singledispatch


@singledispatch
def serialization(t):
    raise NotImplemented


@serialization.register(Task)
def _(t):
    return taskSchema.dump(t)


@serialization.register(Mastertask)
def _(t):
    return masterTaskschema.dump(t)


@serialization.register(SqueueRow)
def _(t):
    return squeueRowSchema.dump(t)


def deserialization(t):
    if t=="starts":
        pass

    elif set(t.keys()).issubset(set(taskSchema.declared_fields.keys())):
        return Task(**taskSchema.load(t))

    elif set(t.keys()).issubset(set(masterTaskschema.declared_fields.keys())):
        return Mastertask(**masterTaskschema.load(t))

    elif set(t.keys()).issubset(set(squeueRowSchema.declared_fields.keys())):
        return SqueueRow(**squeueRowSchema.load(t))

