data = {
    "__task__": True,
    "desc": "a new recon task",
    "data": {
        "filename": "new.h5"
    },
    "state": "submit",
    "workdir": "/home/twj2417/Destop",
    "worker": "1",
    "father": [1],
    "type": "float",
    "dependency": ["task1", "task2"],
    "time_stamp": {
        "create": "2018-05-24 11:55:41.600000",
        "start": None,
        "end": None
    },
    "is_root": False,
    "script_file": [],
    "info": {}
}
newdata = {
    "__task__": True,
    "id": 1,
    "desc": "a new recon task",
    "data": {
        "filename": "new.h5"
    },
    "state": "submit",
    "workdir": "/home/twj2417/Destop",
    "worker": "1",
    "father": [1],
    "type": "float",
    "dependency": ["task1", "task2"],
    "time_stamp": {
        "create": "2018-05-24 11:55:41.600000",
        "start": "2018-05-24 11:56:12.300000",
        "end": "2018-05-26 11:59:23.600000"
    },
    "is_root": False,
    "script_file": ["file.exe"],
    "info": {"job_id": 5826,
             "partition": "main",
             "name": "run.sh",
             "user": "root",
             "status": "PD",
             "time": "0:00",
             "nodes": 1,
             "node_list": "(None)"
             }
}

noupdate_result_data = {
    "__task__": True,
    "id": 1,
    "desc": "a new recon task",
    "data": {
        "filename": "new.h5"
    },
    "worker": "1",
    "type": "float",
    "workdir": "/home/twj2417/Destop",
    "dependency": ["task1", "task2"],
    "father": [1],
    "time_stamp": {
        "create": "2018-05-24 11:55:41.600000",
        "start": None,
        "end": None
    },
    "state": "submit",
    "is_root": False,
    "script_file": [],
    "info": {}
}
