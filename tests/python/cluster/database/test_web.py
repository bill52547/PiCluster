import copy
import pytest
import requests
from dxl.cluster.database.model import Database
from dxl.cluster.database.api.web import *
from dxl.cluster.database.api.cli.cmds import *
from .data_for_test import *

task_db_primary_key = 1


def tasks_url():
    return 'http://localhost:23300/api/v0.2/tasks'


def task_url(tid):
    return 'http://localhost:23300/api/v0.2/task/{}'.format(tid)


@pytest.fixture()
def setup_teardown():
    Database.clear()
    c['path'] = ':memory:'
    Database.create()
    indata = json.dumps(data)
    # DBprocess.create_session()
    # tid = DBprocess.create(indata)
    result = requests.post(tasks_url(), {'task': indata}).json()
    tid = result['id']
    yield tid
    requests.delete(task_url(tid))
    # DBprocess.clear_session()
    Database.clear()
    c.back_to_default()


def test_TaskResource_get(setup_teardown):
    result = requests.get(task_url(tid=setup_teardown))
    assert result.status_code == 200
    assert result.headers['Content-Type'] == "application/json"
    result_data = copy.deepcopy(data)
    result_data['id'] = task_db_primary_key
    assert result.json() == result_data


def test_TaskResource_get2(setup_teardown):
    tid = 0
    resutl = requests.get(task_url(tid))
    assert resutl.status_code == 404
    assert resutl.headers['Content-Type'] == "application/json"
    assert resutl.json() == "Task with id: {tid} not found.".format(tid=tid)


def test_TaskResource_delete(setup_teardown):
    result = requests.delete(task_url(tid=setup_teardown))
    assert result.status_code == 200
    result = requests.get(task_url(tid=setup_teardown))
    assert result.status_code == 404
    assert result.headers['Content-Type'] == "application/json"
    assert result.json() == "Task with id: {tid} not found.".format(tid=setup_teardown)


def test_TaskResource_delete2(setup_teardown):
    result = requests.delete(task_url(tid=0))
    assert result.status_code == 404
    assert result.json() == "Task with id: {tid} not found.".format(tid=0)


def test_TasksResource(setup_teardown):
    result = requests.get(tasks_url())
    assert result.status_code == 200
    assert result.headers['Content-Type'] == "application/json"
    result = result.json()
    result_data = json.loads(result[0])
    maybe_data = copy.deepcopy(data)
    maybe_data['id'] = task_db_primary_key
    assert result_data == maybe_data


def test_TasksResource_post():
    indata = json.dumps(data)
    result = requests.post(tasks_url(), {'task': indata})
    assert result.status_code == 201
    assert result.headers['Content-Type'] == "application/json"
    tid = result.json()
    result = requests.get(task_url(tid['id']))
    assert result.status_code == 200
    assert result.headers['Content-Type'] == "application/json"
    result_data = copy.deepcopy(data)
    result_data['id'] = tid['id']
    assert result.json() == result_data

    requests.delete(task_url(tid))
    DBprocess.clear_session()
    Database.clear()
    c.back_to_default()


def test_TasksResource_put(setup_teardown):
    indata = json.dumps(newdata)
    result = requests.put(tasks_url(), {'task': indata})
    assert result.status_code == 201
    assert result.headers['Content-Type'] == "application/json"
    result = requests.get(task_url(tid=newdata['id']))
    assert result.json() == newdata


def test_TasksResource_put2(setup_teardown):
    copy_newdata = copy.deepcopy(newdata)
    copy_newdata['id'] = 0
    indata = json.dumps(copy_newdata)
    result = requests.put(tasks_url(), {'task': indata})
    assert result.status_code == 404
    assert result.json() == "Task with id: {tid} not found.".format(tid=copy_newdata['id'])


def test_api_root():
    assert api_path('task') == '/api/vNone/task'
    assert api_root(0.2) == '/api/v0.2'


@pytest.mark.parametrize("url_input, expected",
                         [(api_path('task', version='0.2'), '/api/v0.2/task'),
                          (api_path('task', version='0.2', base='database'), '/api/v0.2/database/task'),
                          (api_path('task', version='0.2', base='/database', suffix='<id>'),
                           '/api/v0.2/database/task/<id>'),
                          (api_path('task', version='0.2', base='/', suffix='<id>'), '/api/v0.2/task/<id>'),
                          (api_path('tasks', version='0.2', base='database/'), '/api/v0.2/database/tasks'),
                          (api_path('tasks', version='0.2', base='/database/'), '/api/v0.2/database/tasks')
                          ])
def test_api_path(url_input, expected):
    assert url_input == expected


@pytest.fixture()
def setup_teardown2():
    Database.clear()
    c['path'] = ':memory:'
    Database.create()
    indata = json.dumps(data)
    DBprocess.create_session()
    tid = DBprocess.create(indata)
    yield tid
    # DBprocess.clear_session()
    Database.clear()
    c.back_to_default()


app = Flask(__name__)
api = Api(app)
add_api(api)


def test_add_api(setup_teardown2):
    url = '/api/v{version}/{name}/{id}'.format(version=c['version'], name=c['name'], id=setup_teardown2)
    result = app.test_client().get(url)
    assert result.status_code == 200


def test_add_api2(setup_teardown2):
    url = '/api/v{version}/{name}/{id}'.format(version=c['version'], name=c['name'], id=setup_teardown2)
    result = app.test_client().options(url)
    assert result.status_code == 200


def test_add_api3(setup_teardown2):
    url = '/api/v{version}/{name}/{id}'.format(version=c['version'], name=c['name'], id=setup_teardown2)
    result = app.test_client().head(url)
    assert result.status_code == 200


def test_add_api4(setup_teardown2):
    url = '/api/v{version}/{names}'.format(version=c['version'], names=c['names'])
    result = app.test_client().head(url)
    assert result.status_code == 200


def test_add_api5(setup_teardown2):
    url = '/api/v{version}/{names}'.format(version=c['version'], names=c['names'])
    indata = json.dumps(newdata)
    result = app.test_client().put(url, data={'task': indata})
    assert result.status_code == 201


def test_add_api6(setup_teardown2):
    url = '/api/v{version}/{name}/{id}'.format(version=c['version'], name=c['name'], id=setup_teardown2)
    result = app.test_client().delete(url)
    assert result.status_code == 200


def test_add_api7(setup_teardown2):
    url = '/api/v{version}/{names}'.format(version=c['version'], names=c['names'])
    result = app.test_client().options(url)
    assert result.status_code == 200


def test_add_api8(setup_teardown2):
    url = '/api/v{version}/{names}'.format(version=c['version'], names=c['names'])
    indata = json.dumps(data)
    result = app.test_client().post(url, data={'task': indata})
    assert result.status_code == 201


def test_add_api9(setup_teardown2):
    url = '/api/v{version}/{names}'.format(version=c['version'], names=c['names'])
    result = app.test_client().get(url)
    assert result.status_code == 200
