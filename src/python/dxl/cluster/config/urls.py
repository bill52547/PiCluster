from functools import partial
from ..config import WebConfig


def api_root(ip, port):
    return f"http://{ip}:{port}/"


def _req_url(ip, port, name):
    return api_root(ip, port)+str(name)


task_req_url = partial(_req_url, WebConfig.POSTGREST_IP, WebConfig.POSTGREST_PORT)


def req_slurm(*arg, **kwargs):
    url = f'http://202.120.1.61:1888/api/v1/slurm/{arg[0]}?'
    kvs = []
    for k, v in kwargs.items():
        kvs.append(f"{k}={v}")
    return url + "&".join(kvs)
