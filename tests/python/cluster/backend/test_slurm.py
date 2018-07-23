import unittest
from unittest.mock import patch, Mock
from dxl.cluster.backend import slurm
from dxpy.filesystem import Directory, File
from fs.memoryfs import MemoryFS
from dxl.cluster.interactive.base import Task, State,Type

# value2 = ["             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)             \n",
#           "                327      main  test.sh hongxwing  R       4:41      1 NB408A-WS1    "]
# value3 = ["             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)             \n",
#           "                327      main  test.sh hongxwing  R       4:41      1 NB408A-WS1    ",
#           "                329     main  test.sh hongxwing  R       4:41      1 NB408A-WS1    ",
#           "                400      main  test.sh hongxwing  R       4:41      1 NB408A-WS1    "]


# class TestSlurmHelperFunctions(unittest.TestCase):
#     def setUp(self):
#         pass

#     def tearDown(self):
#         pass

#     @patch('dxl.cluster.backend.slurm._apply_command', return_value=['Submitted batch job 327'])
#     def test_sbatch(self, _apply_command):
#         mfs = MemoryFS()
#         d = Directory('test', mfs)
#         s = File('test/run.sh', mfs)
#         sid = slurm.sbatch(d, s)
#         _apply_command.assert_called_with("cd test && sbatch test/run.sh")

#     @patch('dxl.cluster.backend.slurm._apply_command', return_value=value2)
#     def test_squeue(self, _apply_command):
#         tasks_gathered = slurm.squeue().to_list().to_blocking().first()
#         _apply_command.assert_called_with('squeue')
#         self.assertEqual(tasks_gathered[0].sid, 327)
#         self.assertEqual(tasks_gathered[0].partition, 'main')
#         self.assertEqual(tasks_gathered[0].command, 'test.sh')
#         self.assertEqual(tasks_gathered[0].usr, 'hongxwing')
#         self.assertEqual(tasks_gathered[0].statue, slurm.SlurmStatue.Running)

#     @patch('dxl.cluster.backend.slurm._apply_command', return_value=value3)
#     def test_is_complete(self, _apply_command):
#         self.assertTrue(slurm.is_complete(300))
#         self.assertFalse(slurm.is_complete(327))
#         self.assertFalse(slurm.is_complete(329))
#         self.assertFalse(slurm.is_complete(400))

#     def test_sid_for_submit(self):
#         msg = 'Submitted batch job 327'
#         self.assertEqual(slurm.sid_from_submit(msg), 327)

#     @patch('dxl.cluster.backend.slurm._apply_command', return_value=['Submitted batch job 117909'])
#     def test_sbatch_with_depens(self, _apply_command):
#         mfs = MemoryFS()
#         info = {'depens': [117927, 117928]}
#         t = slurm.TaskSlurm(File('test/run.sh', mfs),
#                             Directory('test', mfs),
#                             info=info)
#         sid = slurm.sbatch(t.work_directory, t.script_file,
#                            *(slurm.dependency_args(t)))
#         _apply_command.assert_called_with(
#             "cd test && sbatch --dependency=afterok:117927:117928 test/run.sh")


# class TestSlurm(unittest.TestCase):
#     def test_dependency_args(self):
#         mfs = MemoryFS()
#         info = {'sid': 117929, 'partition': 'gpu', 'command': 'run.sh', 'usr': 'hongxwin',
#                 'statue': 'PD', 'run_time': '0:00', 'nb_nodes': 1,
#                 'node_list': '(None)', 'depens': [117928]}
#         t = slurm.TaskSlurm(File('test/run.sh', mfs),
#                             Directory('test', mfs),
#                             info=info)
#         self.assertEqual(slurm.dependency_args(t),
#                          ('--dependency=afterok:117928',))

info = slurm.TaskSlurmInfo('main','post.sh','twj','R','0.00',1,'(None)')

task= slurm.TaskSlurm(['run.sh'],tid = 1,is_root=True,workdir='/mnt/gluster/twj/GATE/16',ttype=Type.Script)


class TestSlurm(unittest.TestCase):
    def setUp(self):
        t = slurm.sbatch(task.workdir,task.script_file[0])
        self.id = t


    def tearDown(self):
        slurm.scancel(self.id)
        

    def test_squeue(self):
        slurm.scancel(self.id)
        result = slurm.squeue().to_list().to_blocking().first()
        assert len(result)==0


    def test_scontrol(self):
        state = slurm.scontrol(int(self.id))['job_state']
        assert state=='PENDING'
