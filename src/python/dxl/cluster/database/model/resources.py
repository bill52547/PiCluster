from pathlib import Path
from functools import reduce
from operator import and_
import hashlib
from .schema import resources
from ...interactive.web import Request


def recur_dir(root: "str or Path") -> "List[Dir]":
    result = []

    def _no_sub_dir(root):
        return reduce(and_, [i.is_file() for i in root.iterdir()])

    def _recur_dir(root):
        if not isinstance(root, Path):
            root = Path(root)

        for sub in root.iterdir():
            if sub.is_dir() and _no_sub_dir(sub):
                result.append(sub)
            else:
                _recur_dir(sub)

    _recur_dir(root)
    return result


def recur_file(root: "str or Path") -> "List[File]":
    result = []

    def _recur_file(root):
        if not isinstance(root, Path):
            root = Path(root)

        for sub in root.iterdir():
            if sub.is_file():
                result.append(sub)
            elif sub.is_dir():
                _recur_file(sub)

    _recur_file(root)
    return result


def parse_resource(directory: Path, root="/mnt/gluster/resources"):
    directory = Path(directory)

    def _parse_dir(directory: Path):
        files = list(directory.iterdir())

        try:
            directory = directory.relative_to(root)
        except Exception as e:
            print(f"{e}: Only accept resources under directory: {root}")

        directory_in_list = str(directory).split('/')

        return {"type": directory_in_list[0],
                "comments": "_".join(directory_in_list[1:]),
                "hash": _resource_hash(files),
                "urls": _urls_to_string(files)}

    def _resource_hash(files: "List[Path]"):
        def _hash(url):
            BLOCKSIZE = 65536
            hasher = hashlib.sha1()

            with open(url, 'rb') as afile:
                buf = afile.read(BLOCKSIZE)
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = afile.read(BLOCKSIZE)
            return hasher.hexdigest()

        files_hash = list(map(_hash, files))
        files_hash.sort()
        return "".join(files_hash)

    def _urls_to_string(urls):
        if isinstance(urls, str):
            return "{" + str(urls) + "}"
        elif isinstance(urls, list):
            return "{" + ", ".join([str(f) for f in urls]) + "}"

    return _parse_dir(directory)


def sync_resource(resource_dict):
    duplicated_item_id = Request.read(table_name="resources",
                                      select="hash",
                                      condition=resource_dict["hash"],
                                      returns=["id"])

    modified_item_id = Request.read(table_name="resources",
                                    select="urls",
                                    condition=resource_dict["urls"],
                                    returns=["id"])

    if len(duplicated_item_id):
        raise ValueError(f"Duplicated item with resource id: {duplicated_item_id}.")

    if len(modified_item_id):
        for i in modified_item_id:
            if i in modified_item_id and i not in duplicated_item_id:
                Request.updates(table_name="resources",
                                id=i,
                                patches=resource_dict)
                raise ValueError(f"Modified item with resource id: {modified_item_id}, already updated.")

    return Request.insert(table_name="resources", inserts=resource_dict)


def sync_resources(work_dir):
    #TODO: error info for illegal resource dir not coherent
    resources = recur_dir(work_dir)

    for r in resources:
        try:
            result = sync_resource(parse_resource(r))
            print(result)
        except Exception as e:
            print(e)
            pass

# def insert_all(db):
#     conn = db.get_or_create_engine().connect()
#
#     resource = [
#         {"file_name": "main.mac", "type": "mac", "comments": "16module_x1", "urls": , "hash": }
#         {"file_name":, "type":, "comments":, "urls":, "hash":}
#         {"file_name":, "type":, "comments":, "urls":, "hash":}
#     ]

    # for item in ['slurm', 'shell']:
    #     conn.execute(backends.insert().values(backend=item))
    #
    # procs = [
    #     ("make_subdir", ["pygate", "init", "subdir", "-n"]),
    #     ("bcast", ["pygate", "init", "bcast"]),
    #     ("pygate_submit", ["pygate", "submit"]),
    #     ("h52lor", ["python", "listmode2lor.py"]),
    #     ("h52sino", ["None"]),
    #     ("listmodetxt2h5", ["python", "txt2h5.py"]),
    #     ("root2listmodebintxt", ["bash", "root2bintxt.sh"])
    #     # "sino_lor"    "{python,lor_sino.py,arg1_config,arg2_input,arg3_output,arg4_type}"
    # ]

    # for row in procs:
    #     conn.execute(procedures.insert().values(procedure=row[0], command=row[1]))
    #
    # io_files = [
    #     ("GateMaterials.db", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/GateMaterials.db"),
    #     ("Materials.xml", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Materials.xml"),
    #     ("pygate.yml", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/pygate.yml"),
    #     ("run.sh", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/run.sh"),
    #     ("Verbose.mac", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Verbose.mac"),
    #     ("post.sh", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/post.sh"),
    #     ("Hits2CSV.C", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Hits2CSV.C"),
    #     ("Surfaces.xml", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Surfaces.xml")
    # ]
    #
    # for row in io_files:
    #     conn.execute(ioCollections.insert().values(file_name=row[0], comments=row[1], url=row[2]))
    #
    # my_mac = [
    #     ("main.mac", "zql_16Module_x1", "/mnt/gluster/qinglong/macset/mac_sub1/main.mac"),
    #     ("main.mac", "zql_16Module_x2", "/mnt/gluster/qinglong/macset/mac_sub2/main.mac"),
    #     ("main.mac", "zql_16Module_x4", "/mnt/gluster/qinglong/macset/mac_sub3/main.mac")
    # ]
    #
    # for row in my_mac:
    #     conn.execute(macs.insert().values(file_name=row[0], comments=row[1], url=row[2]))
    #
    # my_phantom_header = [
    #     ("header_phantomD.h33", "zql_16Module_10_layers", "/mnt/gluster/qinglong/macset/mac_sub1/header_phantomD.h33")
    # ]
    #
    # for row in my_phantom_header:
    #     conn.execute(phantomHeaders.insert().values(file_name=row[0], comments=row[1], url=row[2]))
