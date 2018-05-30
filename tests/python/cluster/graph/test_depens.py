import unittest
import rx
from dxl.cluster.interactive import base
from dxl.cluster.taskgraph.depens import DepensGraph


class TestDepensGraph(unittest.TestCase):
    def test_init(self):
        g = DepensGraph([1, 2, 3], [None, 1, 4])
        self.assertEqual(list(g.g.nodes()), [1, 2, 3])
        self.assertEqual(list(g.g.successors(1)), [])
        self.assertEqual(list(g.g.successors(2)), [1])
        self.assertEqual(list(g.g.successors(3)), [])

    def test_init_2(self):
        g = DepensGraph([], [])
        self.assertEqual(len(g.g.nodes()), 0)

    def test_add_node(self):
        g = DepensGraph([], [])
        self.assertEqual(len(g.g.nodes()), 0)
        g.add_node(1)
        self.assertEqual(len(g.g.nodes()), 1)
        self.assertIn(1, g.g.nodes())

    def test_remove_node(self):
        g = DepensGraph([1], [None])
        self.assertEqual(list(g.g.nodes()), [1])
        g.remove_node(1)
        self.assertEqual(list(g.g.nodes()), [])

    def test_free_nodes(self):
        g = DepensGraph([1, 2, 3], [None, 1, None])
        self.assertEqual(g.free_nodes(), [1, 3])

    def test_nodes(self):
        g = DepensGraph([1, 2, 3], [None] * 3)
        self.assertEqual(list(g.nodes()), [1, 2, 3])

    def test_is_depens_on(self):
        g = DepensGraph([1, 2, 3], [None, 1, None])
        self.assertTrue(g.is_depens_on(2, 1))
        self.assertFalse(g.is_depens_on(3, 1))

    def test_dependencies(self):
        g = DepensGraph([1, 2, 3], [None, None, [1, 2]])
        self.assertEqual(list(g.dependencies(3)), [1, 2])
