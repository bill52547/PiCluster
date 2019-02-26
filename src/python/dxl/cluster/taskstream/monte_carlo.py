from pathlib import Path
from .cli_tasks import mkdir, mv, cp, mkdir_if_not_exist, mkdir_n_return
from .primitive import Resource, Query, submit, cli, tap
from typing import List
from .combinator import parallel, sequential, parallel_with_error_detect
from ..backend.slurm.slurm import SlurmSjtu
from rx import operators as ops
from functools import partial
import rx
from ..database.model.schema import Task
import arrow


# TODO:
# def mac_generator(work_directory, config: "JSON/Dict") -> Observable[Resource[File]]:
#     return (
#         mkdir_if_not_exist(work_directory)
#         .switch_map(lambda d: create_mac(d, config))
#         .switch_map(lambda f: create(f))
#     )


class MonteCarloSimulation:
    def __init__(
            self,
            work_directory: "Path|str",
            required_resources: 'List[Resource["File"]]',
            nb_subtasks: int,
            scheduler="slurm",
            backend=SlurmSjtu,
            fn='monteCarlo_simu_on_Gate@7.2',
            sub_task_outputs=["result.root"],
            merger_task_output="result.root",
            sub_task_script='run.sh'
    ):
        self.work_directory = work_directory
        self.required_files = required_resources
        self.nb_subtasks = nb_subtasks
        self.sub_task_script = sub_task_script
        self.fn = fn
        self.sub_task_outputs = sub_task_outputs
        self.merger_task_output = merger_task_output
        self.scheduler = scheduler
        self.backend = backend
        self.observable = self._main()

    def _sub_tasks(self):
        return (sequential([mkdir_n_return(self.work_directory),
                            parallel_with_error_detect([self._sub_task(d) for d in self._sub_dirs()])]))

    def _sub_dirs(self):
        return make_sub_directories(root=self.work_directory,
                                    nb_sub_directories=self.nb_subtasks)

    def _sub_task(self, sub_dir):
        return sequential([
            mkdir_n_return(sub_dir),
            self.load_required_files_to_directory
        ]).pipe(ops.last(),
                ops.flat_map(lambda _: self._submit_sub_task(sub_dir)))

    def _merger_task(self, sub_outputs):
        def _submit_merge_task():
            task = Task(workdir=self.work_directory,
                        script="hadd_command",
                        fn=self.fn,
                        outputs=self.merger_task_output,
                        scheduler=self.scheduler)
            return hadd(hadd_output=task.workdir+"/"+task.outputs, sub_outputs=sub_outputs)
        return _submit_merge_task()

    def _submit_sub_task(self, work_dir: str):
        def _sub_task():
            return Task(workdir=work_dir,
                        script=self.sub_task_script,
                        fn=self.fn,
                        outputs=self.sub_task_outputs,
                        scheduler=self.scheduler)

        task = _sub_task()
        return submit(task=task, backend=self.backend)

    def load_required_files_to_directory(self, directory: Path):
        if not isinstance(directory, Path):
            # print(f"DEBUG: load_required_files_to_directory {directory}")
            directory = Path(directory)

        def _load_one_required_resource(resource):
            resource_urls = Query.from_resource(resource).pipe(ops.reduce(lambda x, y: x+y))
            return resource_urls.pipe(ops.flat_map(rx.from_),
                                      ops.flat_map(lambda resource_url: cp(source=resource_url, target=directory)))

        return parallel(tasks=[_load_one_required_resource(resource) for resource in self.required_files])

    def _main(self):
        return self._sub_tasks().pipe(ops.flat_map(self._merger_task))


def make_sub_directories(
        root: Path, nb_sub_directories: int, prefix="sub"
) -> "Observable[List[Path]]":
    """
    usage:
    make_sub_directories(root=".", nb_sub_directories=10).subscribe(print, print)
    """
    if not isinstance(root, Path):
        root = Path(root)
    return [root / f"{prefix}.{i}" for i in range(nb_sub_directories)]


def hadd(sub_outputs: list, hadd_output: str):
    def to_string(l):
        return " ".join(l)
    return cli(f"srun hadd {hadd_output} {to_string(sub_outputs)}")

# def create_sub_directories_via_slurm():
#     t = make_sub_directories("./some/path", 10, "slurm_sub")
#     return submit(t, Slurm("192.168.1.131"))


# def run_a_lot_mc_simulations(
#         root: "Path", nb_tasks: int, config
# ) -> List[Resource["File"]]:
#     return sequential(
#         lambda _: make_sub_directories(root, nb_tasks, "parallel_mc"),
#         lambda dirs: parallel(
#             lambda d: [
#                 MonteCarloSimulation(
#                     d, 100, config, [Resource("constant/monte_carlo/material.xml")]
#                 )
#                 for d in dirs
#             ]
#         ),
#     )

# alotmc = run_a_lot_mc_simulations(...)
# submit(alotmc).subscribe_on(Slurm('1545'))
