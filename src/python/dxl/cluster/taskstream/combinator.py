import typing
from .primitive import func
from functools import reduce


def parallel(
    tasks: typing.Sequence[func], error_detectors: typing.Sequence["ErrorDetector"] = []
):
    """
    wrap of rx operators merge
    """
    return merge(*tasks, *[d(tasks) for d in error_detectors])


def sequential(tasks_maker: typing.Callable[['T'], typing.Sequence[func]]):
    """
    """
    return reduce(lambda obs, maker: obs.switch_map(maker))


def average_time_detector(ratio: float = 3.0) -> "ErrorDetector":
    """
    throws errors if one of observed tasks runs for ratio * mean_time_of(80% tasks)
    """
    return lambda tasks: Observable['Done'] # for throw purpose only
