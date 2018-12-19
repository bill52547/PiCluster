import rx
from

class taskStreamFactory:
    def __init__(self, task_input, task_out, task_err, task_result):
        self.task_input = task_input
        self.task_out = task_out
        self.task_err = task_err
        self.task_result = task_result

    def __new__(cls, *args, **kwargs):

