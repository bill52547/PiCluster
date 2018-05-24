import yaml
from functools import wraps
from .exceptions import UnknownConfigName

VERSION = 0.2

_default = {
    'name': 'task',
    'names': 'tasks',
    'version': 0.2,
}


class DatabaseConfigs:
    def __init__(self, file=None, root=None, ip=None, port=None, version=None, name=None, use_web_api=False):
        self.file = file or '/home/hongxwing/Workspace/databases/tasksv2.db'
        self.root = root or 'sqlite:///'
        self.ip = ip or '127.0.0.1'
        self.port = port or 23301
        self.echo = False
        self.version = version or VERSION
        self.name = name or 'taskdb'
        self.use_web_api = use_web_api
        self.debug = False

    @property
    def path(self):
        return self.root + self.file

    @property
    def task_url(self):
        return '/api/v{version}/{name}'.format(
            version=self.version, name=self.name)

    @property
    def tasks_url(self):
        return '/api/v{version}/{name}s'.format(
            version=self.version, name=self.name)


class InterfaceConfigs:
    def __init__(self, ip=None, port=None, name=None):
        self.ip = ip or '0.0.0.0'
        self.port = port or 23302
        self.name = name or 'task'
        self.version = VERSION
        self.debug = False

    @property
    def task_url(self):
        return '/api/v{version}/{name}'.format(
            version=self.version, name=self.name)

    @property
    def tasks_url(self):
        return '/api/v{version}/{name}s'.format(
            version=self.version, name=self.name)


CONFIGS = {
    'database': None,
    'interface': None,
}

CONFIGS_CLS = {
    'database': DatabaseConfigs,
    'interface': InterfaceConfigs,
}


def name_check(func):
    @wraps(func)
    def wrapper(name, *args, **kwargs):
        if name is not None and name not in CONFIGS:
            raise UnknownConfigName(name)
        return func(name, *args, **kwargs)
    return wrapper


@name_check
def get_config_cls(name):
    return CONFIGS_CLS[name]


@name_check
def get_config(name):
    if CONFIGS[name] is None:
        CONFIGS[name] = get_config_cls(name)()
    return CONFIGS[name]


@name_check
def set_config(name, yml_file=None, config=None):
    if yml_file is not None:
        CONFIGS[name] = yaml.load(yml_file)
    elif config is not None:
        CONFIGS[name] = config


@name_check
def set_config_by_name_key(name, key, value):
    c = get_config(name)
    setattr(c, key, value)


@name_check
def clear_config(name='all'):
    if name.upper() == 'ALL':
        for k in CONFIGS:
            CONFIGS[k] = None
    else:
        CONFIGS[name] = None
