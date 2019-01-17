import click
from dxl.cluster.interactive.web import Request
import os

# def auto_sub():
#     from ..conf import config, KEYS
#     if config.get(KEYS.SUB_PATTERNS) is None:
#         config[KEYS.SUB_PATTERNS] = [config.get(KEYS.SUB_PREFIX) + '*']
#         config[KEYS.SUB_FORMAT] = config.get(KEYS.SUB_PREFIX) + r'.{}'


# def load_config(filename, is_no_config, dryrun):
#     from ..conf import config, KEYS
#     import yaml
#     import json
#     if dryrun is not None:
#         config['dryrun'] = dryrun
#     if not is_no_config and filename is not None:
#         with open(filename, 'r') as fin:
#             if filename.endswith('yml'):
#                 config.update(yaml.load(fin))
#             else:
#                 config.update(json.load(fin))
#     auto_sub()


# @click.group()
# @click.option('--config', help="config file name", default=None)
# def cli(config):
#     click.echo(config)
#     click.echo(os.getcwd())


@click.command()
def cli():
    """Example script."""
    click.echo('Hello World!')
