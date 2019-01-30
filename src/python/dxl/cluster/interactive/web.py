from functools import wraps
from typing import List
import requests
import json
import rx
from jinja2 import Environment, FileSystemLoader, select_autoescape
from typing import Iterable

from .templates import query_update, query_conditional_read, query_insert, query_read
from ..database.model import TaskState, Task, Mastertask
from ..database.model import taskSchema
from ..config import WebConfig, GraphQLConfig
from ..database.transactions import deserialization
from functools import partial


# def connection_error_handle(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         try:
#             return func(*args, **kwargs)
#         except requests.ConnectionError as e:
#             raise Exception("Task database server connection failed. Details:\n{e}".format(e=e))
#     return wrapper


class Request:
    j2_env = Environment(
        loader=FileSystemLoader('./template'),
        autoescape=select_autoescape(['j2'])
    )

    @staticmethod
    def url():
        return GraphQLConfig.GraphQL_URL

    @classmethod
    def run_query(cls, query):
        """A function to use requests.post to make the API call. Note the json= section."""
        request = requests.post(cls.url(), json={'query': query})
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

    @classmethod
    def updates(cls, table_name, id, patches):
        j2_template = cls.j2_env.from_string(query_update)
        query = j2_template.render(table_name=table_name, id=id, patches=patches)
        return cls.run_query(query)

    @classmethod
    def read(cls, table_name: str, select: str, condition: str, returns: Iterable[str], operator="_eq"):
        """select & condition can be none."""
        def parse(response):
            return [[row[key] for key in row] for row in response['data'][table_name]]

        if select is not None and condition is not None:
            j2_template = cls.j2_env.from_string(query_conditional_read)
            query = j2_template.render(table_name=table_name,
                                       select=select,
                                       operator=operator,
                                       condition=condition,
                                       returns=returns)
            return parse(cls.run_query(query))
        else:
            j2_template = cls.j2_env.from_string(query_read)
            query = j2_template.render(table_name=table_name, returns=returns)
            return cls.run_query(query)

    @classmethod
    def read_tasks(cls, taskid_list):
        if isinstance(taskid_list, int):
            taskid_list = [taskid_list]
        if isinstance(taskid_list, list):
            tmp = []
            response = Request.read(table_name='tasks',
                                    select='id',
                                    operator='_in',
                                    returns=taskSchema.declared_fields.keys(),
                                    condition=taskid_list)
            for t in response['data']['tasks']:
                tmp.append(deserialization(t))

            return tmp
        else:
            raise TypeError

    @classmethod
    def insert(cls, table_name: str, inserts: dict):
        j2_template = cls.j2_env.from_string(query_insert)
        query = j2_template.render(table_name=table_name, inserts=inserts)
        return cls.run_query(query)

    @classmethod
    def get_submitable(cls, tasks_to_track=1):
        #TODO 序列化/反序列化  还用原来方法做

        query_template = """
            query {
              masterTask(
                limit: {{tasks_to_track}}
                where: {state: {_eq: "Created"}}
              ){
                id
                tasksBytaskId{
                  id
                  depends
                }
              }
            }
        """
        j2_template = cls.j2_env.from_string(query_template)
        query = j2_template.render(tasks_to_track=tasks_to_track)
        response = cls.run_query(query)

        d = {}

        for t in response['data']['masterTask']:
            d[t['tasksBytaskId']['id']] = t['tasksBytaskId']['depends']

        return d

# class Request:
#
#     @staticmethod
#     def url_rpc_call(func):
#         return f"{WebConfig.POSTGREST_RPC_URL}{func}"
#
#     @classmethod
#     @connection_error_handle
#     def create(cls, task):
#         task_json = task.to_json()
#         r = requests.post(cls._url_task, json=task_json).json()
#         task.id = r['id']
#         return task
#
#     @classmethod
#     @connection_error_handle
#     def delete_cascade(cls, taskSimu_id):
#         requests.delete(cls._url_postgrest + f"/taskSimu?and=(id.eq.{taskSimu_id})")
#
#     @classmethod
#     @connection_error_handle
#     def read(cls, task_id: int):
#         result = json.loads(requests.get(cls._url_postgrest + f"/tasks?and=(id.eq.{task_id})").text)
#         return rx.Observable.from_(result).map(lambda t: Task(**taskSchema.load(t)))
#
#     @classmethod
#     @connection_error_handle
#     def read_all(cls):
#         task_json = json.loads(requests.get(cls._url_postgrest + f"/tasks?").text)
#         return rx.Observable.from_list(task_json).map(lambda t: Task(**taskSchema.load(t)))
#
#     @classmethod
#     @connection_error_handle
#     def read_state(cls, state: int):
#         task_json = json.loads(requests.get(cls._url_postgrest + f'/tasks?and=(state.eq.{TaskState(state).name})').text)
#         return rx.Observable.from_list(task_json).map(lambda t: Task(**taskSchema.load(t)))
#
#     @classmethod
#     @connection_error_handle
#     def read_taskSimu(cls, id=None):
#         if id is None:
#             response = json.loads(requests.get(cls._url_postgrest_taskSimu).text)
#             return rx.Observable.from_(response).map(lambda t: Mastertask(**masterTaskchema.load(t)))
#
#         response = json.loads(requests.get(cls._url_postgrest_taskSimu + f"?and=(id.eq.{id})").text)
#         return rx.Observable.from_(response).map(lambda t: Mastertask(**masterTaskchema.load(t)))
#
#     @classmethod
#     @connection_error_handle
#     def taskSimu_depends(cls, taskSimu_id: int):
#         querystring = {"tasksimu_id": str(taskSimu_id)}
#         result = requests.get(cls.url_rpc_call('tasksimu_depends'), params=querystring).text
#         return rx.Observable.from_([(i['task_id'], i['task_state']) for i in json.loads(result)])
#
#     @classmethod
#     @connection_error_handle
#     def depends_completion(cls, taskSimu_id: int):
#         """
#         Number of not completed tasks of a taskSimu.
#         """
#         querystring = {"tasksimu_id": str(taskSimu_id)}
#         result = requests.get(cls.url_rpc_call('depends_completion'), params=querystring).text
#         return rx.Observable.from_(result)
#
#     @classmethod
#     @connection_error_handle
#     def tasksimu_to_check(cls, num_limit: int):
#         """
#         taskSimus under tracking.
#         """
#         querystring = {"num_limit": str(num_limit)}
#         result = requests.get(cls.url_rpc_call("tasksimu_to_check"), params=querystring).text
#         return rx.Observable.from_([i["tasksimu_id"] for i in json.loads(result)])
#
#     @classmethod
#     @connection_error_handle
#     def read_from_list(cls, task_ids: List[int]):
#         if len(task_ids) != 0:
#             querystring = {"ids": str(set(task_ids))}
#             response = requests.get(cls.url_rpc_call("read_from_list"), params=querystring).text
#             return rx.Observable.from_(json.loads(response)).map(lambda t: Task(**taskSchema.load(t))) #yo
#         else:
#             return ""
#
#     @classmethod
#     @connection_error_handle
#     def task_to_taskslurm(cls, task_ids: List[int]):
#         if len(task_ids) != 0:
#             querystring = {"ids": str(set(task_ids))}
#             response = requests.get(cls.url_rpc_call("task_to_taskslurm"), params=querystring).text
#             return (rx.Observable.from_(json.loads(response))
#                     .map(lambda t: SlurmTask(**slurmTaskSchema.load(t))))
#         else:
#             return ""
#
#     @classmethod
#     @connection_error_handle
#     def tasksimu_id_cast(cls, taskSimu_id: int):
#         querystring = {"tasksimu_id": str(taskSimu_id)}
#         r = requests.get(cls.url_rpc_call("tasksimu_id_cast"), params=querystring).text
#         return rx.Observable.from_(json.loads(r)).map(lambda d: d["task_id"])
#
#     @classmethod
#     @connection_error_handle
#     def dependency_checking(cls, task_id: int):
#         querystring = {"task_id": str(task_id)}
#         r = requests.get(cls.url_rpc_call("dependency_checking"), params=querystring).text
#         return rx.Observable.from_([int(r)])
#
#     @classmethod
#     @connection_error_handle
#     def patch_taskSlurm(cls, taskSlurm_id: int, patch: dict):
#         querystring = {"id": f"eq.{taskSlurm_id}"}
#         requests.patch(cls._url_postgrest_taskSlurm, params=querystring, data=patch)
#
#     @classmethod
#     @connection_error_handle
#     def patch(cls, task_id: int, patch: dict):
#         querystring = {"id": f"eq.{task_id}"}
#         requests.patch(cls._url_postgrest_tasks, params=querystring, data=patch)
