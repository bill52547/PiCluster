import rx
from rx import operators as ops
import os
import json
import yaml
import requests
import shutil
from pathlib import Path
from yaml import Loader

from dxl.cluster.interactive.web import Request
from dxl.cluster.database.transactions import deserialization
from .schema import SlurmOp
from ..base import Backend
from ...database.model.schema import Task
from ...config.slurm import SlurmConfig


class Slurm(Backend):
    def __init__(self, ip, port, api_version, *args, **kwargs):
        self.ip = ip
        self.port = port
        self.api_version = api_version

    def url(self, *arg, **kwargs):
        url = f'http://{self.ip}:{self.port}/api/v{self.api_version}/slurm/{arg[0]}?'
        kvs = []
        for k, v in kwargs.items():
            kvs.append(f"{k}={v}")
        return url + "&".join(kvs)

    def _squeue(self):
        response = requests.get(self.url(SlurmOp.squeue.value)).text
        return json.loads(response)

    def _scancel(self, id: int):
        if id is None:
            raise ValueError
        return requests.delete(self.url(SlurmOp.scancel.value, job_id=id)).text

    def _scontrol(self, id: int):
        def query(observer, scheduler):
            try:
                result = requests.get(self.url(SlurmOp.scontrol.value, job_id=id)).json()
                observer.on_next(result['job_state'])
                observer.on_completed()
            except:
                pass
        return rx.create(query)

    def queue(self):
        return rx.interval(1.0).pipe(ops.map(lambda _: self._squeue()))

    def completed(self):
        def squeue_scanner(last, current):
            last_running, _ = last
            result = (current, [i for i in last_running if i not in current])
            return result

        complete_queue = (
            rx.interval(1.0).pipe(
                ops.map(lambda _: self._squeue()),
                ops.map(lambda l: [deserialization(i).job_id for i in l]),
                ops.scan(squeue_scanner, ([], [])),
                ops.map(lambda x: x[1]),
                ops.filter(lambda x: x != [])
            )
        )
        return complete_queue

    def submit(self, task: 'Task'):
        def _sbatch():
            work_dir = task.workdir
            file = task.script

            arg = file
            _url = self.url(SlurmOp.sbatch.value, arg=arg, file=file, work_dir=work_dir)
            result = requests.post(_url).json()
            return result['job_id']
        return _sbatch()

    def cancel(self, task: 'Task'):
        if isinstance(task, Task):
            response = self._scancel(task.id_on_backend)
        elif isinstance(task, int):
            response = self._scancel(task)

        return str(response)


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
    for row in query_config:
        response = Request.read(table_name=row[0],
                                select=row[1],
                                condition='"'+str(row[2])+'"',
                                returns=row[4])
    return response


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


complete_queue = (
    rx.interval(1.0).pipe(
        ops.map(lambda _: SlurmSjtu.queue()),
        ops.map(lambda l: [deserialization(i).job_id for i in l]),
        ops.scan(squeue_scanner, ([], [])),
        ops.map(lambda x: x[1]),
        ops.filter(lambda x: x!=[])
    )
)

SlurmSjtu = Slurm(ip=SlurmConfig.RestSlurm_IP,
                  port=SlurmConfig.RestSlurm_Port,
                  api_version=SlurmConfig.Api_Version)

