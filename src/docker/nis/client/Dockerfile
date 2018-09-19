FROM ubuntu:16.04
RUN apt-get update && apt-get -y install nis
ADD yp.conf /etc/yp.conf
ADD nsswitch.conf /etc/nsswitch.conf
RUN mkdir /run/sendsigs.omit.d/
ENV RUNLEVEL 1
RUN sed -i "s/^exit 101$/exit 0/" /usr/sbin/policy-rc.d

#ENTRYPOINT ["/etc/init.d/nis", "restart"]
#RUN echo "NISDOMAIN=cluster0.teci-pi.com" >> /etc/sysconfig/network
#RUN systemctl enable rpcbind ypbind
#RUN authconfig --enablenis --nisdomain=cluster0.tech-pi.com --nisserver=nis.tech-pi.com --enablemkhomedir --update
#CMD ["/usr/sbin/init"]
#RUN ypdomainname cluster-nb410.tech-pi.com 
#RUN echo "NISDOMAIN=cluster-nb410.tech-pi.com " >> /etc/sysconfig/network 
#RUN systemctl enable rpcbind ypserv ypxfrd yppasswd


