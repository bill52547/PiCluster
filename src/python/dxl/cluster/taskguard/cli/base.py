import click
import time

@click.group()
def run():
    pass


@click.command()
def start():
    """ start task database api service """
    from ..cycle import CycleService
    CycleService.start()
    while True:
        time.sleep(100)


run.add_command(start)