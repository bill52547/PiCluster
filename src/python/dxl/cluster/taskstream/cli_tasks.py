from .primitive import cli, Resource, func
from typing import Sequence
from pathlib import Path


def ls(source=".") -> func:
    return cli(f"ls {source}")


def mv(source: "File", target: "Path") -> 'func[Resource["File"]]':
    try:
        source = Path(source)
        target = Path(target)
        if source.is_file() and target.is_dir():
            return cli(f"mv {source} {target}")
        else:
            raise ValueError
    except Exception as e:
        print(e)


def mkdir(target: "Path") -> 'func[Resource["Path"]]':
    if not isinstance(target, str):
        try:
            target = str(target)
        except Exception as e:
            print(e)

    return cli("mkdir "+target)


def cp(source: "File", target: "Path") -> 'func[Resource["File"]]':
    try:
        source = Path(source)
        target = Path(target)
        if source.is_file() and target.is_dir():
            return cli(f"cp {str(source)} {str(target)}")
        else:
            raise ValueError
    except Exception as e:
        print(e)


def rm(target: "File") -> 'func[Resource["File"]]':
    try:
        target = Path(target)
        if target.is_file():
            return cli(f"rm {str(target)}")
        if target.is_dir():
            return cli(f"rm -r {str(target)}")
        else:
            raise ValueError
    except Exception as e:
        print(e)
