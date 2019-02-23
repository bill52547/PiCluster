from .primitive import cli, Resource, func
from .combinator import sequential
from rx import operators as ops
import rx
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
            raise e

    return cli("mkdir "+target)


def mkdir_n_return(target_sub: "Path") -> "Observable[Path]":
    def _to_return(pwd):
        if Path(target_sub).is_absolute():
            return target_sub
        elif not Path(target_sub).is_absolute():
            return pwd+"/"+target_sub

    return (sequential([mkdir(target_sub), cli('pwd')])
            .pipe(ops.map(_to_return)))


def mkdir_if_not_exist(target: "Path") -> 'func[Resource["Path"]]':
    if Path(target).is_dir():
        return ls(target)
    return mkdir_n_return(target)


def cp(source: "url", target: "url") -> 'func[Resource["File"]]':
    if Path(source).is_file() and Path(target).is_dir():
        return cli(f"cp {str(source)} {str(target)}")
    else:
        raise ValueError(f"Resource: {source} is not a file, or target {target} is not a dir.")

# def cp(source: "url", target: "url") -> 'func[Resource["File"]]':
#     try:
#         source = Path(source)
#         target = Path(target)
#         print(f"DEBUG: primitive.cp: source {source} isfile: {source.is_file()}, target: {traget} is_dir: {target.is_dir()}")
#
#         if source.is_file() and target.is_dir():
#
#             return cli(f"cp {str(source)} {str(target)}")
#         else:
#             raise ValueError(f"Resource: {source} is not a file, or target {target} is not a dir.")
#     except Exception as e:
#         print(e)


def rm(target: "File") -> 'func[Resource["File"]]':
    try:
        target = Path(target)
    except:
        raise ValueError(f"Target {target} is not an acceptable url.")

    if target.is_dir():
        return cli(f"rm -r {str(target)}")
    return cli(f"rm {str(target)}")
