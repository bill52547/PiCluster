import typing
from .primitive import func
from functools import reduce
from rx import merge
from rx import operators as ops


def parallel(
    tasks: typing.Sequence[func], error_detectors: typing.Sequence["ErrorDetector"] = []
):
    """
    wrap of rx operators merge
    """
    return merge(*tasks, *[d(tasks) for d in error_detectors])


def sequential(seq: "List[submit]"):
    """
    usage:
    parallel([submit_task_1, submit_task_2])
    """
    return reduce(lambda prev, nxt: prev.pipe(ops.flat_map(nxt)), seq)


def average_time_detector(ratio: float = 3.0) -> "ErrorDetector":
    """
    throws errors if one of observed tasks runs for ratio * mean_time_of(80% tasks)
    """
    return lambda tasks: Observable['Done'] # for throw purpose only
