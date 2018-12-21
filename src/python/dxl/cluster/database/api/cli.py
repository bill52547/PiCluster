from flask_restful import Api
from flask import Flask
import click

from dxl.cluster.database.api.tasks import add_resource
from dxl.cluster.database import TaskTransactions, DataBase
from dxl.core.debug import enter_debug


@click.group()
def database():
    pass


@database.command()
def start():
    """ start task database api service """
    enter_debug()
    app = Flask(__name__)
    api = Api(app)
    # TODO move config stuff to a config file
    db = DataBase(passwd='psql', ip='202.120.1.61', port=30002)
    add_resource(api, TaskTransactions(db))
    app.run(host="0.0.0.0", port=23300, debug=True)


if __name__ == "__main__":
    database()