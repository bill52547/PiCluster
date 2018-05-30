"""
Graph with dipendencies
"""

import networkx as nx
from dxpy.exceptions.checks import assert_same_length


class DepensGraph:
    def __init__(self, nodes=None, depens=None):
        """
            n1 depens on n2 if n2 in n1.out_nodes.
        """
        assert_same_length((nodes, depens), ('nodes', 'depens'))
        self.g = nx.DiGraph()
        self.g.add_nodes_from(nodes)
        for i, ds in enumerate(depens):
            self.add_depens(list(self.g.nodes().keys())[i], ds)

    def add_node(self, n):
        self.g.add_node(n)

    def remove_node(self, n):
        self.g.remove_node(n)

    def add_depens(self, node, depens):
        if depens is None or depens == [None]:
            return
        if not isinstance(depens, (list, tuple)):
            self.g.add_edge(node, depens)
            return
        for d in depens:
            self.g.add_edge(node, d)

    def draw(self):
        nx.draw(self.g, with_labels=True)

    def is_free(self, node):
        return self.g.out_degree(node) == 0

    def is_root(self, node):
        return self.g.in_degree(node) == 0

    def free_nodes(self):
        return [n for n in self.g if self.is_free(n)]

    def root_nodes(self):
        return [n for n in self.g if self.is_root(n)]

    def is_depens_on(self, node_succ, node_depended):
        return node_depended in self.g.successors(node_succ)

    def dependencies(self, n):
        return self.g.successors(n)

    def nodes(self):
        return self.g.nodes()

    def __len__(self):
        return len(self.g)
