from .depens import DepensGraph
from ..interactive import web,base
import rx

class Graph(DepensGraph):
    def __init__(self,nodes=None, depens=None):        
        super().__init__(nodes,depens)

    def all_runable(self):
        runable = self.free_nodes()        
        return runable 
        
    def mark_complete(self):
        node = self.nodes()
        node = (rx.Observable.from_(node).filter(lambda t:web.Request().read(t).state==base.State.Complete).to_list()
              .subscribe_on(rx.concurrency.ThreadPoolScheduler())
              .to_blocking().first())
        for i in node:
            self.remove_node(i)