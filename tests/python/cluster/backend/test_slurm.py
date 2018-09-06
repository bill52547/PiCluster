import copy
import unittest
from unittest.mock import patch, Mock

import requests

from dxl.cluster.backend import slurm
from dxpy.filesystem import Directory, File
from fs.memoryfs import MemoryFS

from dxl.cluster.backend.slurm import TaskSlurmInfo, TaskSlurm, Slurm
from dxl.cluster.interactive.base import Task, State, Type, TaskInfo

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
#


dct = {'sid': 117929, 'partition': 'gpu', 'name': 'run.sh', 'user': 'hongxwin',
	   'status': 'PD', 'time': '0:00', 'nodes': 1, 'node_list': '(None)', 'job_id': 1}
new_data = {
	"__task__": True,
	"id": 1,
	"desc": "a new recon task",
	"data": {
		"filename": "new.h5"
	},
	"state": "submit",
	"workdir": "/home/twj2417/Destop",
	"worker": "1",
	"father": [1],
	"type": "float",
	"dependency": ["task1", "task2"],
	"time_stamp": {
		"create": "2018-05-24 11:55:41.600000",
		"start": "2018-05-24 11:56:12.300000",
		"end": "2018-05-26 11:59:23.600000"
	},
	"is_root": False,
	"script_file": ["file.exe"],
	"info": {"job_id": 5826,
			 "partition": "main",
			 "name": "run.sh",
			 "user": "root",
			 "status": "PD",
			 "time": "0:00",
			 "nodes": 1,
			 "node_list": "(None)"
			 }
}
taskslurm_data = {
	'tid': 1,
	"desc": "a new recon task",
	"data": {
		"filename": "new.h5"
	},
	"statue": State.BeforeSubmit,
	"workdir": '/mnt/gluster/twj/GATE/16',
	"father": [1],
	"ttype": Type.Script,
	"dependency": ["task1", "task2"],
	"time_stamp": {
		"create": "2018-05-24 11:55:41.600000",
		"start": "2018-05-24 11:56:12.300000",
		"end": "2018-05-26 11:59:23.600000"
	},
	"is_root": True,
	"script_file": ['run.sh'],
	"info": {
		'job_id': 1,
		'nb_nodes': 0,
		'node_list': [2, 3, 4, 5],
		'nb_GPU': None,
		'args': 'run.sh'
	}
}

info = TaskInfo(1, 1, '(None)', 'R', '0.00', )

# task = slurm.TaskSlurm(['run.sh'], tid=1, is_root=True, workdir='/mnt/gluster/twj/GATE/16', ttype=Type.Script)
task = slurm.TaskSlurm(**taskslurm_data)


# class TestSlurm(unittest.TestCase):
# 	def setUp(self):
# 		# TODO()sbatch: error: Batch job submission failed: Unable to contact slurm controller (connect failure)-->slurm接口未实现，实现了才可以测试
# 		t = slurm.sbatch(task.workdir, task.script_file[0], task.info['args'])
# 		self.id = t
#
# 	def tearDown(self):
# 		slurm.scancel(self.id
# )
#
# 	def test_squeue(self):
# 		slurm.scancel(self.id)
# 		result = slurm.squeue().to_list().to_blocking().first()
# 		assert len(result) == 0
#
# 	def test_scontrol(self):
# 		state = slurm.scontrol(int(self.id))['job_state']
# 		assert state == 'PENDING'
#
# 	def test_get_task_info(self):
# 		slurm_info = slurm.get_task_info(self.id)
# 		assert slurm_info ==


# def test_submit():
# 	result = Slurm.submit(task)
# 	print(result)
#
#
# def test_update():
# 	result = Slurm.update(task)
# 	print(result)
#
#
# def test_cancel():
# 	result = Slurm.cancel(task)


def test_TaskSlurmInfo_parse_dict():
	t1 = TaskSlurmInfo.parse_dict(dct)
	t2 = TaskSlurmInfo.parse_dict(dct)
	assert t1 == t2


def test_TaskSlurmInfo_to_dict():
	t = TaskSlurmInfo.parse_dict(dct)
	new_dct = copy.deepcopy(dct)
	del new_dct['sid']
	assert t.to_dict() == new_dct


def test_TaskSlurmInfo_repr():
	t = TaskSlurmInfo.parse_dict(dct)
	assert repr(t) == 'taskslurm(sid=%s)' % dct['job_id']


def test_TaskSlurm_init():
	t = TaskSlurm(**taskslurm_data)
	assert t.sid == taskslurm_data['info']['job_id']
