FROM ubuntu:16.04
RUN apt-get update && apt-get -y install nis && apt-get clean
RUN mkdir /run/sendsigs.omit.d
ENV RUNLEVEL 1
RUN sed -i "s/^exit 101$/exit 0/" /usr/sbin/policy-rc.d
ADD nis /etc/default/nis
#ADD yp.conf /etc/yp.conf
ADD ypserv.securenets /etc/ypserv.securenets
ADD YPMakefile /var/yp/Makefile
RUN dpkg-reconfigure nis
RUN /etc/init.d/rpcbind restart
RUN /etc/init.d/nis restart
RUN /usr/lib/yp/ypinit -m

