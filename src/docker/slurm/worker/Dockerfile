FROM 192.168.1.133:5000/nis-client
RUN apt-get update && apt-get -y install slurm-llnl
ADD munge.key /etc/munge/munge.key
RUN chown munge:munge /etc/munge/munge.key
RUN chmod go-rw /etc/munge/munge.key
ADD startup.sh /etc/profile.d/
RUN chmod +x /etc/profile.d/startup.sh
ENV SPACK_ROOT=/opt/spack \
    PATH=/opt/conda/bin:/opt/spack/bin:$PATH
RUN echo "/etc/profile.d/startup.sh" >> /root/.bashrc
RUN echo ". /opt/spack/share/spack/setup-env.sh" >> /root/.bashrc
RUN echo "source <(spack module loads --dependencies gate@7.2)" >> /root/.bashrc
RUN echo "source geant4.sh" >> /root/.bashrc
#ENTRYPOINT . /etc/profile.d/startup.sh && /bin/bash


