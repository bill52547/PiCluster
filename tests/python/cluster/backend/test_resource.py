from dxl.cluster.backend import resource

data = {
	'source_CPU': 1,
	'source_GPU': 2,
	'source_memory': 3
}
update_data = {
	'source_CPU': 4,
	'source_GPU': 5,
	'source_memory': 6
}


def test_Resource():
	t = resource.Resource(**data)

	assert t.cpu_source == data['source_CPU']
	assert t.gpu_source == data['source_GPU']
	assert t.mem_source == data['source_memory']

	t2 = t.update_CPU(update_data['source_CPU'])
	assert t2.cpu_source == update_data['source_CPU']

	t2 = t.update_GPU(update_data['source_GPU'])
	assert t2.gpu_source == update_data['source_GPU']

	t2 = t.update_MEM(update_data['source_memory'])
	assert t2.mem_source == update_data['source_memory']

# TODO()
# def test_resource.allocate_node(2)
# 	pass
