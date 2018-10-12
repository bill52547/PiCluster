FROM ubuntu:16.04

# install phase
RUN apt-get update && apt-get install -y slurm-llnl

# add phase
ADD munge.key /etc/munge/munge.key
ADD startup.sh /etc/profile.d/
ADD ./rest-slurm /bin/rest-slurm

# run phase
RUN chown munge:munge /etc/munge/munge.key && \
    chmod go-rw /etc/munge/munge.key && \
    chmod +x /bin/rest-slurm && \
    chmod +x /etc/profile.d/startup.sh


# env phase
ENV SPACK_ROOT=/opt/spack \
    PATH=/opt/conda/bin:/opt/spack/bin:$PATH

#ENTRYPOINT rest-slurm
