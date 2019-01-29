import arrow

from ..interactive.web import Request
from ..database.model.schema import taskSchema
from ..database.model import TaskState
from ..database.transactions import serialization
from ..backend.slurm.slurm import sbatch


to_submit_dict = Request.get_submitable()

for i in to_submit_dict:
    resp = Request.read(table_name='tasks',
                        select='id',
                        operator='_in',
                        condition=str(to_submit_dict[i]),
                        returns=list(taskSchema.declared_fields.keys()))


def patch_task(t):
    t_serialized = serialization(t)
    #     print(t_serialized)

    Request.updates(table_name='tasks',
                    id=t.id,
                    patches={
                        'state_on_backend': t_serialized['state_on_backend'],
                        'submit': t_serialized['submit'],
                        'id_on_backend': t_serialized['id_on_backend']
                    })


def submit(t):
    t.id_on_backend = sbatch(work_dir=t.workdir, file=t.script)
    t.submit = arrow.utcnow().datetime
    t.state_on_backend = TaskState.Submitted.name
    patch_task(t)
    return t


(rx.Observable.from_(resp['data']['tasks'])
 .map(deserialization)
 .map(submit))