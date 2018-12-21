from enum import Enum, auto


class Backend:
    def update(self, task: 'Task') -> 'Task':
        """
        Update task statues, thus reading true current statue of Task from backend scheduler system.
        Idemopotent. Pure for dxcluster system, not global pure.
        """
        raise NotImplementedError

    def submit(self, task: 'Task') -> 'Task':
        raise NotImplementedError
    
    def cancel(self, task: 'Task') -> 'Task':
        raise NotImplementedError

    def is_failure(self, task: 'Task') -> 'Bool':
        raise NotImplementedError


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class Backends(AutoName):
    Slurm = auto()