from .schema import backends, ioCollections
# from ...interactive import TaskTransactions


# backends.insert().value(backend='slurm')

# fileCollections = Table(

#                                             Column('mac', String, ForeignKey("macs.mac")),
#     Column('post_script', Boolean),
#     Column('run_script', Boolean),
#     Column('material_db', Boolean),
#     Column('verbose_mac', Boolean),
                                        #     Column('phantom_bin', Boolean),
#     Column('header_phantom', Boolean),
#     Column('activity_range', Boolean),
#     Column('range_material', Boolean),
#     Column('err', Boolean),
#     Column('out', Boolean),
#     Column('root', Boolean)
# )


# procedureCollections = Table(
#     'procedureCollections', meta,
#     Column('id', Integer, primary_key=True),
#     Column('subdir_init', Boolean),
#     Column('bcast', Boolean),
#     Column('run_pygate_submit', Boolean),
#     Column('run_slurm', Boolean),
#     Column('run_gate', Boolean),
#     Column('run_root_to_listmode_bin', Boolean),
#     Column('run_listmode_bin_to_listmode_h5', Boolean),
#     Column('run_stir', Boolean),
#     Column('run_listmode_to_lor', Boolean),
#     Column('run_lor_to_sinogram', Boolean),
#     Column('run_sinogram_to_lor', Boolean),
#     Column('run_lor_recon', Boolean),
#     Column('run_srf_recon', Boolean)
# )



def insert_all(db):
    conn = db.get_or_create_engine().connect()

    for item in ['slurm', 'bare']:
        conn.execute(backends.insert().values(backend=item))

    io_files = [
        ("GateMaterials.db", "gate material database", "/mnt/gluster/qinglong/macset/mac_sub1/GateMaterials.db"),
        ("Materials.xml", "materials", "/mnt/gluster/qinglong/macset/mac_sub1/Materials.xml"),
        ("pygate.yml", "configuration for pygate", "/mnt/gluster/qinglong/macset/mac_sub1/pygate.yml"),
        ("run.sh", "sub tasks runner", "/mnt/gluster/qinglong/macset/mac_sub1/run.sh"),
        ("Verbose.mac", "Verbose", "/mnt/gluster/qinglong/macset/mac_sub1/Verbose.mac"),
        ("post.sh", "merge tasks runner", "/mnt/gluster/qinglong/macset/mac_sub1/post.sh"),
        ("Hits2CSV.C", "Hits2CSV", "/mnt/gluster/qinglong/macset/mac_sub1/Hits2CSV.C"),
        ("Surfaces.xml", "Surfaces", "/mnt/gluster/qinglong/macset/mac_sub1/Surfaces.xml")
    ]

    for row in io_files:
        conn.execute(ioCollections.insert().values(file_name=row[0], comments=row[1], url=row[2]))

    task_ops = [
        ("mksubdir", "pygate.yml", ""),
        ("bcast", "pygate.yml", ""),
        ()
    ]