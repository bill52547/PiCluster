from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
import attr
import typing

import contextlib
import yaml


def load_config(fn='./config.yml'):
    try:
        with open(fn, 'r') as fin:
            return yaml.load(fin)
    except FileNotFoundError as e:
        return {}


def engine():
    config = load_config()
    user = config.get('user', 'postgres')
    passwd = config.get('passwd', 'mysecretpassword')
    database = config.get('database', 'postgres')
    ip = config.get('ip', '127.0.0.1')
    port = config.get('port', '8080')
    return create_engine(f"postgresql://{user}:{passwd}@{ip}:{port}/{database}")


def maker(eng):
    return scoped_session(sessionmaker(eng))


class DataBase:
<<<<<<< HEAD:src/python/dxl/cluster/database2/db.py
    def __init__(self, user: str = 'postgres', passwd: str = 'mysecretpasswd',
                 database: str = 'postgres',
                 port: str = '8080'):
        self.user = user
        self.passwd = passwd
        self.database = database
        self.port = port
        self.maker = None
        self.engine = None
=======
    user: str = 'postgres'
    passwd: str = 'mysecretpassword'
    database: str = 'postgres'
    ip: str = '127.0.0.1'
    port: str = '8080'
    maker: typing.Any = None
    engine: typing.Any = None
>>>>>>> task-refactor:src/python/dxl/cluster/database/db.py

    def get_or_create_engine(self):
        if self.engine is None:
            self.engine = create_engine(f"postgresql://{self.user}:{self.passwd}@{self.ip}:{self.port}/{self.database}")
        return self.engine

    def get_or_create_maker(self):
        if self.maker is None:
            # self.maker = scoped_session(sessionmaker(self.get_or_create_engine()))
            self.maker = sessionmaker(self.get_or_create_engine())
        return self.maker

    @contextlib.contextmanager
    def session(self):
        sess = self.get_or_create_maker()()
        try:
            yield sess
            # sess.expunge_all()
        except Exception as e:
            sess.rollback()
            raise e
        finally:
            sess.close()
