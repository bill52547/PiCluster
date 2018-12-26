from functools import partial


def api_root(ip, port):
    return f"http://{ip}:{port}/"


def _req_url(ip, port, name):
    return api_root(ip, port)+str(name)


req_url = partial(_req_url, "202.120.1.61", "3000")

# _url_postgrest = "http://202.120.1.61:3000"
# _url_postgrest_tasks = "http://202.120.1.61:3000/tasks"
# _url_postgrest_taskSlurm = "http://202.120.1.61:3000/taskSlurm"
# _url_postgrest_taskSimu = "http://202.120.1.61:3000/taskSimu"

