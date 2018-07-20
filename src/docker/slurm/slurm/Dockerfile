FROM 192.168.1.133:5000/spack
RUN yum -y install git curl
RUN yum -y groupinstall "Development Tools"
ENV SPACK_ROOT /spack
ADD startup.sh /etc/profile.d
ENV PATH="$SPACK_ROOT/bin:${PATH}"
ENV SLURM_CONF=/etc/slurm-llnl/slurm.conf


