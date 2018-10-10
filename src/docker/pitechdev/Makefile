#repo = 192.168.1.133:5000
repo = hub.tek-pi.com/env
name = pitechdev
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
	sudo docker run --network host -v $(SPACK_DEVELOP_ROOT):/opt/spack -it --rm $(repo)/$(name)
test :
	sudo docker run -v $(SPACK_DEVELOP_ROOT):/opt/spack -it --rm ubuntu:16.04
static:
	sudo docker run --name dev-zql  --network host -v $(SPACK_DEVELOP_ROOT):/opt/spack -v /mnt:/mnt -p 22000:22  -it -d  $(repo)/$(name)
runs:
	sudo docker exec -it dev-zql bash
