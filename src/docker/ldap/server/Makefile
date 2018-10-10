repo = hub.tek-pi.com/env
name = ldap-server
VERSION := $(shell cat version | tr -d '\n')

image :
	sudo docker build -t $(repo)/$(name):$(VERSION) -t $(repo)/$(name):latest .
push :
	sudo docker push $(repo)/$(name):latest
pull :
	sudo docker pull $(repo)/$(name):latest
dev :
	sudo docker run -it --rm $(repo)/$(name)
ongit :
	sudo docker run --name my-openldap-container --detach -it --rm osixia/openldap:1.2.2
