from .base import Task,State,Worker,Type
#from .api import submit,start,complete
import json
import time


class TaskSleep(Task):
    def __init__(self,tid=None,desc=None,workdir=None,worker=None,ttype=None,state=None,time_stamp=None,dependency=None,is_root=None,data=None):
        super().__init__(tid=tid,desc=desc,workdir=workdir,worker=worker,ttype=ttype,father=None,state=state,time_stamp=time_stamp,dependency=dependency,is_root=is_root,data=data,script_file=None,info=None)
        
    def update(self,tid=None,desc=None,workdir=None,worker=None,ttype=None,state=None,
                time_stamp=None,dependency=None,is_root=None,data=None):
        if tid is None:
            tid = self.id
        if desc is None:
            desc = self.desc  
        if workdir is None:
            workdir = self.workdir
        if worker is None:
            worker = self.worker
        if ttype is None:
            ttype = self.type
        if state is None:
            state = self.state
        if time_stamp is None:
            time_stamp = self.time_stamp
        if dependency is None:
            dependency = self.dependency
        if is_root is None:
            is_root = self.is_root
        if data is None:
            data = self.data
        return TaskSleep(tid,desc,workdir,worker,ttype,state,time_stamp,dependency,is_root,data)
    
    def submit(self):
        return self.update(state= State.Pending)
            
    def run(self):
        time.sleep(10)
        t = self.update(state= State.Complete)
        return t



    
