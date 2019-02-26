# import rx
# import typing
# from typing import Generic, Callable
#
#
# T = typing.TypeVar("T")
# R = typing.TypeVar("R")
# E = typing.TypeVar("E")
#
#
# class Observer(Generic[T, E]):
#     def on_next(self, x: T):
#         pass
#
#     def on_error(self, error: E):
#         pass
#
#     def on_complete(self):
#         pass
#
#
# class Observable(Generic[T]):
#     def subscribe(self, observer: Observer[T]):
#         pass
#
#     def map(self, fn: Callable[[T], R]) -> Observable[R]:
#         pass
#
#     def switch_map(self, fn: Callable[[T], Observable[R]]) -> Observable[R]:
#         pass

