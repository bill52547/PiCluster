from .tasks import backends, ioCollections


backends.insert().value(backend='slurm')

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
    conn = db.engine.connect()

    for item in ['slurm', 'bare']:
        conn.execute(backends.insert().value(backend=item))

    for item in ['post_script', 'run_script', 'material_db', 'verbose_mac', 'header_phantom']:
        conn.execute(ioCollections.insert().value())


