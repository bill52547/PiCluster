from .rxpi import Observable, Observer
from .combinator import sequential
from .cli_tasks import mkdir, mv, cp
from .primitive import Resource, query
from typing import List
from .combinator import parallel, sequential, average_time_detector


def mac_generator(work_directory, config: "JSON/Dict") -> Observable[Resource[File]]:
    return (
        mkdir_if_not_exist(work_directory)
        .switch_map(lambda d: create_mac(d, config))
        .switch_map(lambda f: create(f))
    )


class MonteCarloSimulation(Observable[Resource["Sinogram | ListMode | ..."]]):
    def __init__(
        self,
        work_directory,
        nb_subtasks,
        config,
        required_files: List[Resource["File"]],
    ):
        self.work_directory = work_directory
        self.required_files = required_files
        self.nb_subtasks = nb_subtasks
        self.config = config
        self.observable = self._main()

    def _main_long(self):
        return (
            mkdir(self.work_directory)
            .switch_map(lambda d: make_sub_directories(d, self.nb_subtasks, "sub"))
            .switch_map(
                lambda sub_dirs: parallel(
                    [
                        work_in_one_subdirectory(d, self.required_files)
                        for d in sub_dirs
                    ],
                    [average_time_detector],
                )
            )
            .switch_map(lambda results: merge(results))
            .switch_map(lambda root: analysis(root))
            .switch_map(lambda sino: create(sino))
        )

    def _main(self):
        return sequential(
            lambda _: mkdir(self.work_directory),
            lambda d: make_sub_directories(d, self.nb_subtasks, "sub"),
            lambda sub_dirs: parallel(
                [work_in_one_subdirectory(d, self.required_files) for d in sub_dirs],
                [average_time_detector],
            ),
            merge,
            analysis,
            create,
        )

    def subscribe(self, observer: Observer):
        return self.observable.subscribe(observer)


def work_in_one_subdirectory(
    directory: "Path", required_files: List[Resource["File"]], config
):
    return (
        parallel(
            [query(r).switch_map(lambda f: cp(f, directory / f.name)) for r in required_files]
            + [mac_generator(directory, config)]
        )
        .switch_map(lambda files: submit(Gate(*files), Slurm("192.168.1.131")))
        .switch_map(
            lambda slurm_id: slurm_watcher.filter(lambda complted: slurm_id in complted)
        ).switch_map(lambda _: search_file('sino.h5')) # maybe add this into submit func?
    )


def make_sub_directories(
    root: "Path", nb_sub_directories: int, prefix="sub"
) -> Observable[List["Path"]]:
    return cli.mkdir(root).switch_map(
        lambda d: parallel(
            [cli.mkdir(d / f"{prefix}.{i}") for i in range(nb_sub_directories)]
        )
    )


def create_sub_directories_via_slurm():
    t = make_sub_directories("./some/path", 10, "slurm_sub")
    return submit(t, Slurm("192.168.1.131"))


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

alotmc = run_a_lot_mc_simulations(...)
submit(alotmc).subscribe_on(Slurm('1545'))