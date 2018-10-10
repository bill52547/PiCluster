repo = hub.tek-pi.com/env
name = slurm-ctrl
VERSION := $(shell cat version | tr -d '\n')
image :
	sudo cp ../munge.key .
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
run :
	sudo docker run -h $(name) --network=host \
                                   -v $(SPACK_ROOT):/opt/spack \
                                   -v /home/qinglong/dxcluster/src/k8s/slurm-conf:/etc/slurm-llnl/ \
                                   -v /tmp/slurmtest:/tmp/ \
                                   -it --rm $(repo)/$(name)
