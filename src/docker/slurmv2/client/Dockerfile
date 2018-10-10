FROM ubuntu:16.04

# install phase
RUN apt-get update && apt-get install -y nis slurm-llnl

# add phase
ADD yp.conf /etc/yp.conf
ADD nsswitch.conf /etc/nsswitch.conf
ADD munge.key /etc/munge/munge.key
ADD startup.sh /etc/profile.d/
ADD ./rest-slurm /bin/rest-slurm

# run phase
RUN mkdir /run/sendsigs.omit.d/
RUN sed -i "s/^exit 101$/exit 0/" /usr/sbin/policy-rc.d
RUN chown munge:munge /etc/munge/munge.key && \
    chmod go-rw /etc/munge/munge.key && \
    chmod +x /bin/rest-slurm && \
    chmod +x /etc/profile.d/startup.sh
RUN echo "/etc/profile.d/startup.sh" >> /root/.bashrc && \
    echo ". /opt/spack/share/spack/setup-env.sh" >> /root/.bashrc && \
    echo "source <(spack module loads --dependencies gate@7.2)" >> /root/.bashrc && \
    echo "source geant4.sh" >> /root/.bashrc

# env phase
ENV RUNLEVEL 1
ENV SPACK_ROOT=/opt/spack \
    PATH=/opt/conda/bin:/opt/spack/bin:$PATH

#ENTRYPOINT rest-slurm
