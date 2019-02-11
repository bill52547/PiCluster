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

    DB_NAME = 'postgres'
    DB_USER = 'postgres'
    DB_PASSWD = 'psql'
    DB_IP = '202.120.1.61'
    DB_PORT = 30002

    FLASK_IP = '0.0.0.0'
    FLASK_PORT = 23300
    FLASK_DEBUG = True


class GraphQLConfig(Config):
    GraphQL_IP = '192.168.1.133'
    GraphQL_PORT = 8081

    GraphQL_URL = f"http://{GraphQL_IP}:{GraphQL_PORT}/v1alpha1/graphql"


class ConfigFile(Config):
    FileName = "dxclusterConf.yaml"
    CwdConf = './'+FileName
