repo = 192.168.1.133:5000
name = nis-server
VERSION := $(shell cat version | tr -d '\n')
image :
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
run :
	sudo docker run -h nis -it --rm $(repo)/$(name)

