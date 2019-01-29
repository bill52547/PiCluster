import rx
import os

import json
import yaml

import requests

import shutil
from jfs.directory import Directory

from pathlib import Path
from yaml import Loader, Dumper


from dxl.cluster.config.urls import req_slurm
from dxl.cluster.interactive.web import Request
from dxl.cluster.database.transactions import deserialization
from .schema import SlurmOp
# from dxl.cluster.interactive.templates import env, master_task_config


# class SlurmOp(AutoName):
#     scontrol = auto()
#     scancel = auto()
#     squeue = auto()
#     sbatch = auto()
#
#
# class SlurmTaskState(enum.Enum):
#     Canceled = "CA"
#     Created = "PD"
#     Running = "R"
#     Suspend = "S"
#     Completed = "CD"
#     Failed = "F"
#     TimeOut = "TO"
#     NodeFault = "NF"
#
#
# @attr.s(auto_attribs=True)
# class SqueueRow:
#     job_id: typing.Optional[int] = None
#     partition: typing.Optional[str] = None
#     name: typing.Optional[str] = None
#     user: typing.Optional[str] = None
#     status: typing.Optional[str] = None
#     time: typing.Optional[str] = None
#     nodes: typing.Optional[str] = None
#     node_list: typing.Optional[str] = None
#
#     def __eq__(self, other):
#         if isinstance(other, type(self)):
#             self.job_id == other.job_id
#         else:
#             raise TypeError
#
#     def __hash__(self):
#         return hash(self.job_id)
#
#
# class SqueueRowSchema(ma.Schema):
#     job_id = ma.fields.Integer(allow_none=False)
#     partition = ma.fields.String(allow_none=True)
#     name = ma.fields.String(allow_none=True)
#     user = ma.fields.String(allow_none=True)
#     status = ma.fields.String(allow_none=True)
#     time = ma.fields.String(allow_none=True)
#     nodes = ma.fields.String(allow_none=True)
#     node_list = ma.fields.String(allow_none=True)
#
#
# squeueRowSchema = SqueueRowSchema()


# sq = (rx.Observable.interval(1*1000)#.take(10008)
#       .map(lambda _: squeue())
#       .map(lambda l: [deserialization(i).job_id for i in l]))


def squeue():
    response = requests.get(req_slurm(SlurmOp.squeue.value)).text
    return json.loads(response)


def sbatch(work_dir: Directory, file):
    arg = file
    _url = req_slurm(SlurmOp.sbatch.value, arg=arg, file=file, work_dir=work_dir)
    # print(_url)
    result = requests.post(_url).json()
    # print(result)
    return result['job_id']


def scancel(id: int):
    if id is None:
        raise ValueError
    requests.delete(req_slurm(SlurmOp.scancel.value, job_id=id))


def scontrol(id: int):
    def query(observer):
        try:
            result = requests.get(req_slurm(SlurmOp.scontrol.value, job_id=id)).json()
            observer.on_next(result['job_state'])
            observer.on_completed()
        except:
            pass

    return rx.Observable.create(query)


def config_parser(config_dict):
    tmp_outer = []
    for k, v in config_dict.items():
        tmp_inner = []
        tmp_inner.append(k)
        if isinstance(v, dict):
            for k, v_1 in v.items():
                tmp_inner.append(k)
                tmp_inner.append(v_1)
        tmp_outer.append(tmp_inner)
    return tmp_outer


def url_parser(query_config):
    all_inputs = []

    for row in query_config:
        # print(row)
        response = Request.read(table_name=row[0],
                                select=row[1],
                                condition='"'+str(row[2])+'"',
                                returns=row[4])
        for r in response['data'][row[0]]:
            for k, v in r.items():
                all_inputs.append(v)
    return all_inputs


def input_loading(workdir, source):
    if not isinstance(workdir, Path):
        workdir = Path(workdir)
    for f in source:
        if isinstance(f, str):
            f = Path(f)
            if f.parents[0] == workdir:
                pass
            shutil.copyfile(f, workdir/f.name)


def init_with_config(config_url, workdir):
    with open(config_url, 'rt') as f:
        config_data = yaml.load(f.read(), Loader=Loader)

    urls = []
    urls += url_parser(config_parser(config_data['spec']['inputs']))

    input_loading(workdir=workdir, source=urls)
    return urls


def clean_with_config(config_url):
    with open(config_url, 'rt') as f:
        config_data = yaml.load(f.read(), Loader=Loader)
    urls = []
    urls += url_parser(config_parser(config_data['spec']['inputs']))

    for url in urls:
        try:
            os.remove("./"+str(Path(url).name))
            print(f"{'./'+str(Path(url).name)} has been removed")
        except:
            pass


def procedure_parser(conf):
    conf = yaml.load(conf)
    return conf['spec']['procedures']


def squeue_scanner(last, current):
    last_running, _ = last
    result = (current, [i for i in last_running if i not in current])
    return result


complete_queue = (rx.subjects.Subject.interval(1*1000) #.take(80)
                  .map(lambda _: squeue())
                  .map(lambda l: [deserialization(i).job_id for i in l])
                  .scan(squeue_scanner, ([], []))
                  .map(lambda x: x[1])
                  .filter(lambda x: x!=[]))

