from flask_restful import Api
from flask import Flask
import click

from dxl.cluster.database.tasks import add_resource
from dxl.cluster.database import DataBase
from dxl.cluster.interactive import TaskTransactions
from dxl.core.debug import enter_debug
from dxl.cluster.config import DBConfig


@click.group()
def database():
    pass


@database.command()
def start():
    """ start task database api service """
    enter_debug()
    app = Flask(__name__)
    api = Api(app)
    db = DataBase(passwd=DBConfig.DB_PASSWD,
                  ip=DBConfig.DB_IP,
                  port=DBConfig.DB_PORT)
    add_resource(api, TaskTransactions(db))
    app.run(host=DBConfig.FLASK_IP,
            port=DBConfig.FLASK_PORT,
            debug=DBConfig.FLASK_DEBUG)


if __name__ == "__main__":
    database()