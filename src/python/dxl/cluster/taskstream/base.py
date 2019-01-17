import rx
from collections import UserDict


class TaskBase(UserDict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)



class taskStreamFactory:
    def __init__(self):
        self.tasks = []
        # self.task_inputs = []
        # self.task_outputs = []
        # self.task_errs = []
        # self.task_results = []

    def load(self, input, task):
        pass
