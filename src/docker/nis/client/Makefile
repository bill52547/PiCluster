#repo = 192.168.1.133:5000
repo = hub.tek-pi.com/env
name = nis-client
VERSION := $(shell cat version | tr -d '\n')
image :
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
run :
	#sudo docker run -it --network host --rm $(repo)/$(name)
	sudo docker run -it --rm $(repo)/$(name)

