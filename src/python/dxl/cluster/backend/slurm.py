import rx
import json
import requests
from rx import Observable
from jfs.directory import Directory


def scontrol_url(id):
    return f'http://202.120.1.61:1888/api/v1/slurm/scontrol?job_id={id}'


def scancel_url(id):
    return f'http://202.120.1.61:1888/api/v1/slurm/scancel?job_id={id}'


def squeue_url():
    return 'http://202.120.1.61:1888/api/v1/slurm/squeue'


def sbatch_url(args, filename, workdir):
    return f'http://202.120.1.61:1888/api/v1/slurm/sbatch?arg={args}&file={filename}&work_dir={workdir}'


# class SlurmState(Enum):
#     Running = 'R'
#     Completing = 'CG'
#     Completed = 'E'
#     Pending = 'PD'
#     Failed = 'F'


# def slurm_state2task_state(s: SlurmState):
#     return {
#         SlurmState.Running: TaskState.Running,
#         SlurmState.Completing: TaskState.Completed,
#         SlurmState.Completed: TaskState.Completed,
#         SlurmState.Pending: TaskState.Pending,
#         SlurmState.Failed: TaskState.Failed
#     }[s]


# class ScontrolState(Enum):
#     Pending = 'PENDING'
#     Running = 'RUNNING'
#     Suspended = 'SUSPENDED'
#     Complete = 'COMPLETED'
#     Failed = 'FAILED'
#     Canceled = 'CANCELLED'
#     Timeout = 'TIMEOUT'
#     NodeFailed = 'NODE_FAILED'

#
# def scontrol_state2task_state(s: ScontrolState):
#     scontrol2state_mapping = {
#         ScontrolState.Pending: TaskState.Pending,
#         ScontrolState.Running: TaskState.Running,
#         ScontrolState.Suspended: TaskState.Running,
#         ScontrolState.Complete: TaskState.Completed,
#         ScontrolState.Failed: TaskState.Failed,
#         ScontrolState.Canceled: TaskState.Failed,
#         ScontrolState.Timeout: TaskState.Failed,
#         ScontrolState.NodeFailed: TaskState.Failed
#     }
#     return scontrol2state_mapping[ScontrolState(s)]


# def sid_from_submit(s: str):
#     return int(re.sub('\s+', ' ', s).strip().split(' ')[3])


def squeue() -> 'Observable[TaskSlurm]':
    """
    :return: Obserable of tasks retrived from slurm cluter using squeue command.
    """
    infos = requests.get(squeue_url()).text
    info = json.loads(infos)
    return info


def sbatch(workdir: Directory, filename):
    """
    Submitting new task.
    :param workdir:
    :param filename: NOT USED
    :param args: Script file name "run.sh" by default.
    :return:
    """
    # TODO args and filename are set the same. because all developers forget why they were set, too bad, so sad :(
    args=filename
    url_ = sbatch_url(args, filename, workdir)
    result = requests.post(sbatch_url(args, filename, workdir)).json()
    with open('/tmp/output.txt', 'a') as fout:
        print(type(result), result, file=fout)
        try:
            print(result['job_id'], file=fout)
        except Exception as e:
            print(e, file=fout)
            print(result, file=fout)
            raise ValueError(str(e)+"result:"+result+"URL!!!:"+url_)
    return result['job_id']


def scancel(id: int):
    if id is None:
        return False
    requests.delete(scancel_url(id))


def scontrol(id: int):
    def query(observer):
        try:
            result = requests.get(scontrol_url(id)).json()
            observer.on_next(result['job_state'])
            observer.on_completed()
        except:
            pass

    return rx.Observable.create(query)


# def get_state(id: int):
#     if id is None:
#         return False
#     state = scontrol_state2task_state(scontrol(id)['job_state'])
#     return state

#
# def find_id(id):
#     return lambda tinfo: int(tinfo.id) == int(id)
#
#
# def is_end(id):
#     if id is None:
#         return False
#     result = (squeue()
#               .filter(find_id(id))
#               .count().to_list().to_blocking().first())
#     return result[0] == SlurmState.Completed


# def is_complete(id):
#     return is_end(id)


# def get_slurm_info(id: int):
#     result = squeue().filter(find_id(id)).to_list().to_blocking().first()
#     if len(result) == 0:
#         return None
#     return result[0]


# class Slurm(Cluster):
#     """
#     Handle a slurm task life cycle.
#     """
#     @classmethod
#     def submit(cls, t: TaskSlurm):
#         id = sbatch(t.workdir, t.script)
#         slurm_info = get_slurm_info(id)
#         new_info = TaskInfo(id=id,
#                             nb_nodes=slurm_info.nb_nodes,
#                             node_list=slurm_info.node_list)
#
#         new_task = t.update_info(new_info.to_dict())
#         nt = new_task.update_state(TaskState.Runing)
#
#         web.Request().update(nt)
#         return nt
#
#     @classmethod
#     def update(cls, t: TaskSlurm):
#         if t.task_id is None:
#             return t
#         else:
#             state = get_state(t.task_id)
#             nt = t.update_state(state)
#             return nt
#
#     @classmethod
#     def cancel(cls, t: TaskSlurm):
#         scancel(t.id)