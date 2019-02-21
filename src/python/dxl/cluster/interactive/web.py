import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape
from typing import Iterable

from .templates import query_update, query_conditional_read, query_insert, query_read
from ..database.model import taskSchema
from ..config import GraphQLConfig
from ..database.transactions import deserialization


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
        """A function to use requests.post to make the API call. Note the json section."""
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
        """
        Read from taskDB.

        select & condition can be none.
        """
        def parse(response):
            return [[row[key] for key in row] for row in response['data'][table_name]]

        def flat_list(l: "nested_list") -> "List[T]":
            result = []

            def _flatten(l):
                for item in l:
                    if isinstance(item, list):
                        _flatten(item)
                    else:
                        result.append(item)

            _flatten(l)
            return result

        def format_handler(s: str):
            if (s[0] == '"' and s[-1:] == '"') or (s[0] == "'" and s[-1:] == "'"):
                return s[1:-1]
            else:
                return s

        if select is not None and condition is not None:
            j2_template = cls.j2_env.from_string(query_conditional_read)
            query = j2_template.render(table_name=table_name,
                                       select=select,
                                       operator=operator,
                                       condition=format_handler(condition),
                                       returns=returns)
            return flat_list(parse(cls.run_query(query)))
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
