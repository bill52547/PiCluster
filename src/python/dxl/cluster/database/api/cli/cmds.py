import click
from flask_restful import Api
from flask import Flask
from ..web import add_api
from ....config import config as c
import yaml


@click.command()
def start(): 
    """ start task database api service """
    app = Flask(__name__)
    api = Api(app)
    add_api(api)
    app.run(host=c['host'], port=c['port'], debug=c['debug'])

@click.command()
@click.argument("config", type=click.Path(exists=True))
def create(config):
    """
    Create new database.
    :param config: str, YAML config file.
    """
    with open(config, 'r') as fin:
        create_database(yaml.load(fin))
