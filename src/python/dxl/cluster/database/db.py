from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import contextlib

from ..config.base import DBConfig


class DataBase:
    def __init__(self,
                 ip: str = DBConfig.DB_IP,
                 user: str = DBConfig.DB_USER,
                 passwd: str = DBConfig.DB_PASSWD,
                 database: str = DBConfig.DB_NAME,
                 port: str = DBConfig.DB_PORT):
        self.user = user
        self.passwd = passwd
        self.database = database
        self.ip = ip
        self.port = port
        self.maker = None
        self.engine = None

    def get_or_create_engine(self):
        if self.engine is None:
            self.engine = create_engine(f"postgresql://{self.user}:{self.passwd}@{self.ip}:{self.port}/{self.database}")
        return self.engine

    def get_or_create_maker(self):
        if self.maker is None:
            self.maker = sessionmaker(self.get_or_create_engine())
        return self.maker

    @contextlib.contextmanager
    def session(self):
        sess = self.get_or_create_maker()()
        try:
            yield sess
        except Exception as e:
            sess.rollback()
            raise e
        finally:
            sess.close()
