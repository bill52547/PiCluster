repo = 192.168.1.133:5000
name = spack-ubuntu-test
VERSION := $(shell cat version | tr -d '\n')
image :
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
rel :
	sudo docker run --network host -v $(SPACK_ROOT):/opt/spack -it --rm $(repo)/$(name)
dev :
	sudo docker run --network host -v $(SPACK_DEVELOP_ROOT):/opt/spack -v /tmp:/tmp/host -it --rm $(repo)/$(name)

