class Cluster:
    def update(self, task: 'Task') -> 'Task':
        raise NotImplementedError

    def submit(self, task: 'Task') -> 'Task':
        raise NotImplementedError
