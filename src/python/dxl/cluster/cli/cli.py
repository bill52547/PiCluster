import click
import subprocess
from os import getcwd

from ..interactive.templates import env, master_task_config
from ..backend.slurm.slurm import init_with_config, clean_with_config, procedure_parser
from ..config import ConfigFile


@click.group()
def cli():
    pass


@click.command()
@click.option('--backend', default='slurm', help='Which backend will the task based on.')
@click.option('--nb_split', default=None, help='Number of sub tasks.')
@click.option('--mac', help='Retrive mac from DB according given comment.')
@click.option('--phantom_header', help='Retrive phantom header file from DB according given comment.')
@click.option('--phantom_id', help='ID of phantom in the task.')
def init(backend, nb_split, mac, phantom_header, phantom_id):
    template = env.from_string(master_task_config)
    conf = template.render(backend=backend,
                           workdir=getcwd(),
                           mastTaskID=None,
                           nb_split=nb_split,
                           mac=mac,
                           phantom_header=phantom_header,
                           phantom_id=phantom_id)
    print()
    print(conf)
    with open(ConfigFile.CwdConf, 'w') as config_out:
        print(conf, file=config_out)

    init_with_config(config_url=ConfigFile.CwdConf, workdir=getcwd())

    for procedure in procedure_parser(conf):
        print(f"Running: {procedure}")
        subprocess.run(procedure, cwd=getcwd())


@click.command()
def clean():
    clean_with_config('./' + ConfigFile.FileName)
    subprocess.run(["pygate", "clean", "-d"], cwd=getcwd())


cli.add_command(init)
cli.add_command(clean)
