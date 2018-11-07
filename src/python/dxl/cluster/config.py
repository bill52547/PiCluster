import os
from collections import UserDict
from jfs.api import Path
_default = {
    # 'path': str(Path(os.environ.get('PATH_DATABASE')) / 'taskdb.db'),
    # 'path': str(Path('/home/hongjiang/Desktop/dxcluster_config') / 'taskdb.db'),

    'name': 'tasks',
    # 'names': 'tasks',
    'use_web_api': True,
    'echo': False,
    'default_state': 'BeforeSubmit',

    'host': '0.0.0.0',
    'ip': '192.168.1.212',
    # 'port': 23300,
    'port': 23300,
    'debug': False,
    'version': 1,
    'base': '/'
}


class Config(UserDict):
    def __init__(self):
        super(__class__, self).__init__()
        self.data.update(_default)

    # @property
    # def path_sqllite(self):
    #     return 'sqlite:///' + self.data['path']
    #
    # def back_to_default(self):
    #     self.data.update(_default)


config = Config()
