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
#ADD proxychains.conf /etc/proxychains.conf
ENV FORCE_UNSAFE_CONFIGURE 1
RUN apt-get update && apt-get install -y openssh-server
RUN mkdir /var/run/sshd
RUN echo 'root:screencast' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
ENTRYPOINT source /etc/profile.d/startup.sh && /bin/bash
