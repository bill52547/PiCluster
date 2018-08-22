repo = 192.168.1.133:5000
name = spack
VERSION := $(shell cat version | tr -d '\n')
image :
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
run :
	sudo proxychains docker run -v $(SPACK_ROOT):/opt/spack/spack -it --rm $(repo)/$(name)
	#sudo docker run -v $(SPACK_ROOT):/opt/spack/spack -it --rm $(repo)/$(name)

