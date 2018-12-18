FROM ubuntu:16.04
RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN apt-get update && apt-get install -y git curl build-essential nano environment-modules  proxychains gfortran vim python openssh-server
#RUN apt-get -y install gfortran
#RUN apt-get -y install vim
#RUN yum -y groupinstall "Development Tools"
#RUN yum -y install environment-modules
#RUN useradd spack
#USER spack
ADD startup.sh /etc/profile.d
ADD proxychains.conf /etc/proxychains.conf
ENV FORCE_UNSAFE_CONFIGURE 1
ENTRYPOINT source /etc/profile.d/startup.sh && /bin/bash
