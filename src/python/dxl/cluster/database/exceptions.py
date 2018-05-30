class TaskNotFoundError(Exception):
    def __init__(self, tid=None):
        super(__class__, self).__init__(
            "Task with id: {tid} not found.".format(tid=tid))

class InvalidJSONForTask(Exception):
    def __init__(self, details):
        super(__class__, self).__init__(details)