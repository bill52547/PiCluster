import click
from flask_restful import Api
from flask import Flask
from dxl.cluster.database2.api.tasks import add_resource
from dxl.cluster.database2 import TaskTransactions, DataBase
import yaml

@click.group()
def database():
    pass

@database.command()
def start():
    """ start task database api service """
    app = Flask(__name__)
    api = Api(app)
    db = DataBase(passwd='psql', ip='192.168.1.133')
    add_resource(api, TaskTransactions(db))
    app.run(host="192.168.1.212", port=23300, debug=True)

# @click.command()
# @click.argument("config", type=click.Path(exists=True))
# def create(config):
#     """
#     Create new database.
#     :param config: str, YAML config file.
#     """
#     with open(config, 'r') as fin:
#         create_database(yaml.load(fin))

if __name__ == "__main__":
    database()
