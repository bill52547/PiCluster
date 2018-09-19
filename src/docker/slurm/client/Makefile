repo = 192.168.1.133:5000
name = slurm-client
VERSION := $(shell cat version | tr -d '\n')
image :
	sudo cp ../munge.key .
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
run :
	sudo docker run -h $(name) --network=host -v $(SPACK_DEVELOP_ROOT):/opt/spack -v /mnt/gluster/hongxwing/Workspace/dxcluster/src/docker/slurm/config:/etc/slurm-llnl/ -v /tmp/slurmtest/:/tmp/ -it --rm $(repo)/$(name)

