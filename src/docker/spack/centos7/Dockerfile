FROM centos
RUN yum -y install git curl
RUN yum -y groupinstall "Development Tools"
RUN yum -y install environment-modules
#RUN useradd spack
#USER spack
ADD startup.sh /etc/profile.d
