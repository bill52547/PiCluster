from .schema import backends, ioCollections, procedures, macs, phantomHeaders


def insert_all(db):
    conn = db.get_or_create_engine().connect()

    for item in ['slurm', 'bare']:
        conn.execute(backends.insert().values(backend=item))

    procs = [
        ("make_subdir", ["pygate", "init", "subdir", "-n"]),
        ("bcast", ["pygate", "init", "bcast"]),
        ("pygate_submit", ["pygate", "submit"]),
        ("h52lor", ["python", "listmode2lor.py"]),
        ("h52sino", ["None"]),
        ("listmodetxt2h5", ["python", "txt2h5.py"]),
        ("root2listmodebintxt", ["bash", "root2bintxt.sh"])
        # "sino_lor"    "{python,lor_sino.py,arg1_config,arg2_input,arg3_output,arg4_type}"
    ]

    for row in procs:
        conn.execute(procedures.insert().values(procedure=row[0], command=row[1]))

    io_files = [
        ("GateMaterials.db", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/GateMaterials.db"),
        ("Materials.xml", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Materials.xml"),
        ("pygate.yml", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/pygate.yml"),
        ("run.sh", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/run.sh"),
        ("Verbose.mac", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Verbose.mac"),
        ("post.sh", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/post.sh"),
        ("Hits2CSV.C", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Hits2CSV.C"),
        ("Surfaces.xml", "monteCarloSimu", "/mnt/gluster/qinglong/macset/mac_sub1/Surfaces.xml")
    ]

    for row in io_files:
        conn.execute(ioCollections.insert().values(file_name=row[0], comments=row[1], url=row[2]))

    my_mac = [
        ("main.mac", "zql_16Module_x1", "/mnt/gluster/qinglong/macset/mac_sub1/main.mac"),
        ("main.mac", "zql_16Module_x2", "/mnt/gluster/qinglong/macset/mac_sub2/main.mac"),
        ("main.mac", "zql_16Module_x4", "/mnt/gluster/qinglong/macset/mac_sub3/main.mac")
    ]

    for row in my_mac:
        conn.execute(macs.insert().values(file_name=row[0], comments=row[1], url=row[2]))

    my_phantom_header = [
        ("header_phantomD.h33", "zql_16Module_10_layers", "/mnt/gluster/qinglong/macset/mac_sub1/header_phantomD.h33")
    ]

    for row in my_phantom_header:
        conn.execute(phantomHeaders.insert().values(file_name=row[0], comments=row[1], url=row[2]))