FROM 192.168.1.133:5000/nis-client
RUN apt-get update && apt-get -y install slurm-llnl openssh-server
ADD munge.key /etc/munge/munge.key
RUN chown munge:munge /etc/munge/munge.key
RUN chmod go-rw /etc/munge/munge.key
ADD startup.sh /etc/profile.d/

#ADD start-slurm.sh /etc/profile.d/
ADD ./rest-slurm /bin/rest-slurm
RUN chmod +x /bin/rest-slurm
RUN chmod +x /etc/profile.d/startup.sh
#RUN chmod +x /etc/profile.d/start-slurm.sh

ENV SPACK_ROOT=/opt/spack \
    PATH=/opt/conda/bin:/opt/spack/bin:$PATH
RUN echo "/etc/profile.d/startup.sh" >> /root/.bashrc
RUN echo ". /opt/spack/share/spack/setup-env.sh" >> /root/.bashrc
RUN echo "source <(spack module loads --dependencies gate@7.2)" >> /root/.bashrc
RUN echo "source geant4.sh" >> /root/.bashrc
#ENTRYPOINT . /opt/spack/share/spack/setup-env.sh
ENTRYPOINT rest-slurm
