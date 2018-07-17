import re
from enum import Enum
from typing import Dict, Iterable
import numpy as np
import rx

import json

from ..interactive.base import Task, State,Type
from ..interactive import web
from .base import Cluster


#filepath = '/home/twj2417/Desktop/backend.npy'

class TaskSRF(Task):
    def __init__(self,desc,
        work_directory,tid=None,father=None,statue=None,time_stamp=None,
        dependency=None,is_root=True,data=None,worker=None,ttype=None):
        super().__init__(tid=tid,desc=desc,workdir=work_directory,worker=worker,father=father,
                        ttype=ttype,state=statue,time_stamp=time_stamp,dependency=dependency,
                        is_root=is_root,data=data)
    
        
def submit(t:TaskSRF):
    dist_config = t.data['dist']
    recon_in_config = t.data['recon']['config_in']
    recon_out_config = t.data['recon']['config_out']
    model = t.data['algorithm']
    command = 'srf utils {} psf_auto_config -d {} {} {}'.format(model,dist_config,recon_in_config,recon_out_config)    
    web.Request().update(t)
    return t.id


def update(t:TaskSRF):
    data = np.load(filepath)

    if t.id is not None:
        index = np.where(data[:,0]==t.id)
        work_state = data[index,1]
        if work_state==0:
            t.update_state(State.Pending)
        elif work_state==1:
            t.update_state(State.Runing)
        elif work_state==2:
            t.update_state(State.Complete)
        elif work_state==3:
            t.update_state(State.Complete)
        elif work_state==4:
            t.update_state(State.Failed)
    web.Request().update(t)