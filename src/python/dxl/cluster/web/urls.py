from functools import partial


def api_root(ip, port):
    return f"http://{ip}:{port}/"


def _req_url(ip, port, name):
    return api_root(ip, port)+str(name)


task_req_url = partial(_req_url, "202.120.1.61", "3000")


def req_slurm(*arg, **kwargs):
    url = f'http://202.120.1.61:1888/api/v1/slurm/{arg[0]}?'
    for k, v in kwargs.items():
        url += f"{k}={v}"
    return url




# _url_postgrest = "http://202.120.1.61:3000"
# _url_postgrest_tasks = "http://202.120.1.61:3000/tasks"
# _url_postgrest_taskSlurm = "http://202.120.1.61:3000/taskSlurm"
# _url_postgrest_taskSimu = "http://202.120.1.61:3000/taskSimu"

