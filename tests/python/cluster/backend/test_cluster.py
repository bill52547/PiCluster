# import unittest
# from unittest.mock import Mock, patch, create_autospec
# from dxl.cluster import submit_slurm
# from dxl.cluster.backend import slurm
# from dxl.fs import Directory, File
# from fs.memoryfs import MemoryFS
#
#
# # dct = {'sid': 117929, 'partition': 'gpu', 'command': 'run.sh', 'usr': 'hongxwin',
# #        'statue': 'PD', 'run_time': '0:00', 'nb_nodes': 1, 'node_list': '(None)'}
# # dct2 = {'sid': 117930, 'partition': 'gpu', 'command': 'run.sh', 'usr': 'hongxwin',
# #         'statue': 'PD', 'run_time': '0:00', 'nb_nodes': 1, 'node_list': '(None)'}
# dct = {'job_id': 117929, 'partition': 'gpu', 'name': 'run.sh', 'user': 'hongxwin',
#        'status': 'PD', 'time': '0:00', 'nodes': 1, 'node_list': '(None)'}
# dct2 = {'job_id': 117930, 'partition': 'gpu', 'name': 'run.sh', 'user': 'hongxwin',
#         'status': 'PD', 'time': '0:00', 'nodes': 1, 'node_list': '(None)'}
# dummy_info = slurm.TaskSlurmInfo.parse_dict(dct)
# dummy_info2 = slurm.TaskSlurmInfo.parse_dict(dct2)
#
#
# class TestSlurm(unittest.TestCase):
#     @patch('dxl.cluster.backend.slurm.get_task_info', return_value=dummy_info)
#     @patch('dxl.cluster.backend.slurm.sbatch', return_value=117929)
#     def test_sbatch(self, sbatch, get_task_info):
#         mfs = MemoryFS()
#         d = Directory('test', mfs)
#         s = File('test/run.sh', mfs)
#         result = submit_slurm(d, s)
#         sbatch.assert_called_with(d, s)
#         self.assertEqual(result, 117929)
#
#     @patch('dxl.cluster.backend.slurm.get_task_info', return_value=dummy_info)
#     @patch('dxl.cluster.backend.slurm.sbatch', return_value=117929)
#     def test_sbatch_with_dependencies(self, sbatch, get_task_info):
#         mfs = MemoryFS()
#         d = Directory('test', mfs)
#         s = File('test/run.sh', mfs)
#         result = submit_slurm(d, s, [117928])
#         sbatch.assert_called_with(d, s, '--dependency=afterok:117928')
#         self.assertEqual(result, 117929)
#TODO()wait jiaoda's clurm api