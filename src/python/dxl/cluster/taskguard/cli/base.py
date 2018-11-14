import click


@click.group()
def run():
    pass


@click.command()
def start():
    """ start task database api service """
    from ..cycle import CycleService
    CycleService.start()


run.add_command(start)