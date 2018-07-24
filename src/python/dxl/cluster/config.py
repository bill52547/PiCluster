import os
from collections import UserDict
from dxpy.filesystem import Path
_default = {
    'path': str(Path(os.environ.get('PATH_DATABASE')) / 'taskdb.db'),
    'name': 'task',
    'names': 'tasks',
    'use_web_api': True,
    'echo': False,
    'default_state': 'BeforeSubmit',

    'host': '0.0.0.0',
    'ip': '127.0.0.1',
    'port': 23300,
    'debug': False,
    'version': 0.2,
    'base': '/'
}


class Config(UserDict):
    def __init__(self):
        super(__class__, self).__init__()
        self.data.update(_default)

    @property
    def path_sqllite(self):
        return 'sqlite:///' + self.data['path']

    def back_to_default(self):
        self.data.update(_default)


config = Config()
