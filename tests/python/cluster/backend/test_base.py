import json
from dxl.cluster.interactive import base
from dxl.cluster.backend.base import Cluster

dct = {
    '__task__': True,
    'id': 10,
    'desc': 'test',
    'workdir': '/tmp/test',
    'worker': 'Slurm',
    'type': 'Script',
    'dependency': [1, 2, 3],
    'father': [1],
    'data': {'sample': 42},
    'is_root': True,
    'time_stamp': {
        'create': "2017-09-22 12:57:44.036185",
        'start': None,
        'end': None
    },
    'state': 'BeforeSubmit',
    'script_file': ['file1.txt', 'file2.csv', 'file3.pdf'],
    'info': {}
}


def test_Cluster():
    t = base.Task.from_json(json.dumps(dct))
    cluster = Cluster()
    try:
        cluster.update(t)
    except Exception as e:
        assert isinstance(e, NotImplementedError)

    try:
        cluster.submit(t)
    except Exception as e:
        assert isinstance(e, NotImplementedError)

    try:
        cluster.cancel(t)
    except Exception as e:
        assert isinstance(e, NotImplementedError)

    try:
        cluster.is_failure(t)
    except Exception as e:
        assert isinstance(e, NotImplementedError)
