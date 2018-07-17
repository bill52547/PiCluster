import time
import numpy as np
"""
将后端任务执行部分构造成一个假端，此处为npy数组，该数组有两列，
第一列为后端编号sid，第二列为任务状态，
Pending = 0
Runing = 1
Completing = 2
Completed = 3
Failed = 4
"""

def sbatch(workdir,script_file,*args):
    state = np.load('/home/twj2417/Desktop/backend.npy')
    sid = state[:,0]
    i = max(sid)
    return i+1

def is_complete(sid):
    if sid is None:
        return False
    state = np.load('/home/twj2417/Desktop/backend.npy')
    statue = state[sid+1,1]
    if statue == 3:
        return True
