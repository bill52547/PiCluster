FROM ubuntu:16.04
RUN apt-get update && apt-get install -y git curl build-essential python environment-modules nano proxychains
#RUN yum -y groupinstall "Development Tools"
#RUN yum -y install environment-modules
#RUN useradd spack
#USER spack
ADD startup.sh /etc/profile.d
ENTRYPOINT . /etc/profile.d/startup.sh && /bin/bash
