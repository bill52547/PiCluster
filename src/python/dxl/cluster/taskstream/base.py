from collections import UserDict
import abc
from abc import abstractclassmethod
from abc import ABC

#TODO programming socket
class TaskBase(ABC):
    def __init__(self, work_directory, required_resources, scheduler, backend, fn, outputs):
        pass

    def submit(self):
        pass

    def load_required_files_to_work_directory(self):
        pass

    def is_backend_runnable(self):
        pass

    def _main(self):
        pass

    def clean_workdir(self):
        pass

class taskStreamFactory:
    def __init__(self):
        self.tasks = []
        # self.task_inputs = []
        # self.task_outputs = []
        # self.task_errs = []
        # self.task_results = []

    def load(self, input, task):
        pass
