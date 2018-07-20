FROM centos
RUN yum -y install git curl
RUN yum -y groupinstall "Development Tools"
ENV SPACK_ROOT /opt/spack/spack
ENV PATH="$SPACK_ROOT/bin:${PATH}"
ADD startup.sh /etc/profile.d
