from dxl.cluster.web.urls import req_url
import json
import rx
from ..interactive import web,base

total_gpu = 8

class Resource:
    def __init__(self,source_CPU,source_GPU,source_memory):
        self.source_CPU = source_CPU
        self.source_GPU = source_GPU
        self.source_memory = source_memory

    @property
    def cpu_source(self):
        return self.source_CPU
    
    @property
    def gpu_source(self):
        return self.source_GPU

    @property
    def mem_source(self):
        return self.source_memory

    def update_CPU(self,nb_CPU):
        return Resource(nb_CPU,self.source_GPU,self.source_memory)

    def update_GPU(self,nb_GPU):
        return Resource(self.source_CPU,nb_GPU,self.source_memory)

    def update_MEM(self,memory):
        return Resource(self.source_CPU,self.source_GPU,memory)
    

def find_need_gpu():
    return (web.Request().read_all().filter(lambda t:t.is_depen_gpu))


def allocate_node(task):
    num_gpu_used = sum(web.Request().read_all()
          .filter(lambda t:t.is_running or t.is_pending)
          .map(lambda t:t.info['GPUs'])
          .to_list().to_blocking().first())
    num_gpu_left = total_gpu-num_gpu_used
    info= task.info
    if int(info['GPUs'])<=num_gpu_left:
        new_info = base.TaskInfo(sid=info['job_id'],nb_nodes=info['nodes'],node_list='GS0',
                      nb_GPU=info['GPUs'],args='--nodelist=GS0').to_dict()
        return task.update_info(info)
    else:
        return None


    