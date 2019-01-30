from .primitive import cli, Resource, Task
from .rxpi import Observable
from typing import Sequence


def ls(source: "Path" = ".") -> Task[Sequence[str]]:
    return cli("ls")


def mv(source: "File", target: "Path") -> Task[Resource["File"]]:
    return cli(f"mv {source} {target}")


def mkdir(target: "Path") -> Task[Resource["Path"]]:
    pass


def cp(source: "File", target: "Path") -> Task[Resource["File"]]:
    pass

