class Config:
    APP_NAME = 'dxcluster'
    ADMIN_NAME = 'administrator'


class WebConfig(Config):
    DEBUG = True

    POSTGREST_IP = "202.120.1.61"
    POSTGREST_PORT = "3000"

    POSTGREST_RPC_URL = f"http://{POSTGREST_IP}:{POSTGREST_PORT}/rpc/"


class DBConfig(Config):
    DEBUG = True
    TESTING = True

    DEFAULT_CONFIG_FILE = './config.yml'

    DB_USER = 'postgres'
    DB_PASSWD = 'psql'
    DB_IP = '202.120.1.61'
    DB_PORT = 30002

    FLASK_IP = '0.0.0.0'
    FLASK_PORT = 23300
    FLASK_DEBUG = True



# _default = {
#     'name': 'tasks',
#     'use_web_api': True,
#     'echo': False,
#     'default_state': 'Created',
#
#     'host': '0.0.0.0',
#     'ip': '192.168.1.212',
#     'port': 23300,
#     'debug': False,
#     'version': 1,
#     'base': '/'
# }
#
#
# class Config(UserDict):
#     def __init__(self):
#         super(__class__, self).__init__()
#         self.data.update(_default)
#
#
# config = Config()
