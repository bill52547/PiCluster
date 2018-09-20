#repo = 192.168.1.133:5000
repo = hub.tek-pi.com/env
name = slurm-client
VERSION := $(shell cat version | tr -d '\n')
image :
	sudo cp ../munge.key .
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
#run :
#	sudo docker run -h $(name) --network=host -v $(SPACK_DEVELOP_ROOT):/opt/spack -v /mnt/gluster/hongxwing/Workspace/dxcluster/src/docker/slurm/config:/etc/slurm-llnl/ -v /tmp/slurmtest/:/tmp/ -it --rm $(repo)/$(name)

run :
	sudo docker run -h $(name) \
			--network=host \
			-v $(SPACK_DEVELOP_ROOT):/opt/spack \
			-v /mnt/gluster/k8s-pv/slurm-config:/etc/slurm-llnl/\
			-v  /mnt/gluster/k8s-pv/data:/home/data \
			-p 1888:1888 \
			-it --rm $(repo)/$(name)
