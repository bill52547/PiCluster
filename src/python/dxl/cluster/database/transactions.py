from dxl.cluster.database.model import Task, Mastertask, taskSchema, masterTaskschema
from functools import singledispatch


@singledispatch
def serialization(t):
    raise NotImplemented


@serialization.register(Task)
def _(t):
    return taskSchema.dump(t)


# @serialization.register(SlurmTask)
# def _(t):
#     return slurmTaskSchema.dump(t)


@serialization.register(Mastertask)
def _(t):
    return masterTaskschema.dump(t)


def deserialization(t):
    if set(t.keys()).issubset(set(taskSchema.declared_fields.keys())):
        return Task(**taskSchema.load(t))

    # elif set(t.keys()).issubset(set(slurmTaskSchema.declared_fields.keys())):
    #     return SlurmTask(**slurmTaskSchema.load(t))

    elif set(t.keys()).issubset(set(masterTaskschema.declared_fields.keys())):
        return Mastertask(**masterTaskschema.load(t))


