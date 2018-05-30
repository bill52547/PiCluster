import click
from flask_restful import Api
from flask import Flask
from ..web import add_api
from ....config import config as c


@click.command()
def start(): 
    """ start task database web service """
    app = Flask(__name__)
    api = Api(app)
    add_api(api)
    app.run(host=c['host'], port=c['port'], debug=c['debug'])
