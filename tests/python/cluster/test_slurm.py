import unittest
from unittest.mock import Mock
from dxl.cluster.backend import slurm
from dxl.fs import Directory, File
from fs.memoryfs import MemoryFS


class TestSlurm(unittest.TestCase):
    def test_sbatch(self):
        slurm._apply_command = Mock(
            return_value=['Submitted batch job 327'])
        mfs = MemoryFS() 
        d = Directory('test', mfs)
        s = File('test/run.sh', mfs)
        slurm.sbatch(d, s)
        slurm._apply_command.assert_called_with(
            "cd test && sbatch test/run.sh")

    def test_squeue(self):
        slurm_msg = ["             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)             \n",
                     "                327      main  test.sh hongxwing  R       4:41      1 NB408A-WS1    "]
        slurm._apply_command = Mock(return_value=slurm_msg)
        tasks_gathered = slurm.squeue().to_list().to_blocking().first()
        slurm._apply_command.assert_called_with('squeue')
        self.assertEqual(tasks_gathered[0].sid, 327)
        self.assertEqual(tasks_gathered[0].partition, 'main')
        self.assertEqual(tasks_gathered[0].command, 'test.sh')
        self.assertEqual(tasks_gathered[0].usr, 'hongxwing')
        self.assertEqual(tasks_gathered[0].statue, slurm.SlurmStatue.Running)

    def test_is_complete(self):
        slurm_msg = ["             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)             \n",
                     "                327      main  test.sh hongxwing  R       4:41      1 NB408A-WS1    ",
                     "                329     main  test.sh hongxwing  R       4:41      1 NB408A-WS1    ",
                     "                400      main  test.sh hongxwing  R       4:41      1 NB408A-WS1    "]
        slurm._apply_command = Mock(return_value=slurm_msg)
        self.assertTrue(slurm.is_complete(300))
        self.assertFalse(slurm.is_complete(327))
        self.assertFalse(slurm.is_complete(329))
        self.assertFalse(slurm.is_complete(400))

    def test_sid_for_submit(self):
        msg = 'Submitted batch job 327'
        self.assertEqual(slurm.sid_from_submit(msg), 327)
