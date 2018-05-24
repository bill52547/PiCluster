from dxpy.graph.depens import DenpensGraph
from dxpy.exceptions.checks import assert_same_length


def create_graph(task_graph) -> 'list<int>':
    done = []
    for t in task_graph.nodes():
        t.id = None

    def all_dependency_added(task):
        return all([t in done for t in task_graph.dependencies(task)])

    def update_dependency(task):
        task.dependency = [t.id for t in task_graph.dependencies(task)]
        return task

    def add_to_done(task):
        done.append(task)
        return task

    def create_and_update_id(task):
        task.id = create(task)
        return task

    while len(done) < len(task_graph):
        (rx.Observable.from_(task_graph.nodes())
         .filter(lambda t: not t in done)
         .filter(all_dependency_added)
         .map(update_dependency)
         .map(create_and_update_id)
         .map(add_to_done)
         .subscribe())

    return [t.id for t in done]



def task_graph(tasks, depens):
    assert_same_length((tasks, depens), ('tasks', 'depens'))
    depens_tasks = []
    for i, ds in enumerate(depens):
        if ds is None:
            depens_tasks.append([None])
        elif isinstance(ds, int):
            depens_tasks.append([tasks[ds]])
        else:
            depens_tasks.append([tasks[d] for d in ds])
    g = DenpensGraph(tasks, depens_tasks)
    for t in g.nodes():
        t.is_root = g.is_root(t)
    return g