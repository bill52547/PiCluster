from pathlib import Path
from .cli_tasks import mkdir, mv, cp, mkdir_if_not_exist, mkdir_n_return
from .primitive import Resource, Query, submit, cli, tap
from typing import List
from .combinator import parallel, sequential, average_time_detector
from rx import operators as ops
from functools import partial
import rx
from ..database.model.schema import Task
import arrow


# TODO: pygate mac generator not easy to use
# def mac_generator(work_directory, config: "JSON/Dict") -> Observable[Resource[File]]:
#     return (
#         mkdir_if_not_exist(work_directory)
#         .switch_map(lambda d: create_mac(d, config))
#         .switch_map(lambda f: create(f))
#     )


class MonteCarloSimulation:
    def __init__(
            self,
            work_directory,
            required_files: 'List[Resource["File"]]',
            nb_subtasks,
            # config,
            scheduler,
            backend
    ):
        self.work_directory = work_directory
        self.required_files = required_files
        self.nb_subtasks = nb_subtasks
        # self.config = config

        self.sub_task_script = 'run.sh'
        self.merger_task_scirpt = 'post.sh'

        self.fn = 'monteCarlo_simu_on_Gate@7.2'

        self.sub_task_outputs = ["result.root"]
        self.merger_task_output = "result.root"
        self.scheduler = scheduler

        self.backend = backend

        self.observable = self._main()

    def _sub_tasks(self):
        return (sequential([mkdir_n_return(self.work_directory),
                            parallel([self._sub_task(d) for d in self._sub_dirs()])]))

    def _sub_dirs(self):
        return make_sub_directories(root=self.work_directory,
                                    nb_sub_directories=self.nb_subtasks)

    def _sub_task(self, sub_dir):
        return sequential([
            mkdir_n_return(sub_dir),
            self.load_required_files_to_directory,
            self._submit_sub_task
        ])

    def _merger_task(self, sub_outputs):
        def _submit_merge_task():
            task = Task(workdir=self.work_directory,
                        script="hadd_command",
                        fn=self.fn,
                        outputs=self.merger_task_output,
                        scheduler=self.scheduler)
            # return submit(task=task, backend=self.backend)
            print(f"DEBUG task: {task}, sub_outputs: {sub_outputs}")
            return hadd(hadd_output=task.workdir+"/"+task.outputs, sub_outputs=sub_outputs)
        return _submit_merge_task()
        # return sequential([
        #     # rx.of(self.work_directory),
        #     # self.load_required_files_to_directory,
        #     _submit_merge_task
        # ])

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
            directory = Path(directory)

        return (sequential([
            parallel([Query.from_resource(item) for item in self.required_files]),
            lambda sources: parallel([cp(s, directory) for s in sources]),
            rx.of(directory)
        ])).pipe(ops.last())

    def _main(self):
        return self._sub_tasks().pipe(tap(print), ops.reduce(lambda x,y: x+y), ops.flat_map(self._merger_task))


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
    print(f"srun hadd {hadd_output} {to_string(sub_outputs)}")
    return cli(f"srun hadd {hadd_output} {to_string(sub_outputs)}")

# def create_sub_directories_via_slurm():
#     t = make_sub_directories("./some/path", 10, "slurm_sub")
#     return submit(t, Slurm("192.168.1.131"))


def run_a_lot_mc_simulations(
        root: "Path", nb_tasks: int, config
) -> List[Resource["File"]]:
    return sequential(
        lambda _: make_sub_directories(root, nb_tasks, "parallel_mc"),
        lambda dirs: parallel(
            lambda d: [
                MonteCarloSimulation(
                    d, 100, config, [Resource("constant/monte_carlo/material.xml")]
                )
                for d in dirs
            ]
        ),
    )

# alotmc = run_a_lot_mc_simulations(...)
# submit(alotmc).subscribe_on(Slurm('1545'))
