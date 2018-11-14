from collections import UserDict

_default = {
    'name': 'tasks',
    'use_web_api': True,
    'echo': False,
    'default_state': 'Created',

    'host': '0.0.0.0',
    'ip': '192.168.1.212',
    'port': 23300,
    'debug': False,
    'version': 1,
    'base': '/'
}


class Config(UserDict):
    def __init__(self):
        super(__class__, self).__init__()
        self.data.update(_default)


config = Config()
