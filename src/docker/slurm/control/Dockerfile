FROM 192.168.1.133:5000/nis-client
RUN apt-get -y install slurm-llnl openssh-server
#ADD slurm.conf /etc/slurm-llnl/slurm.conf
ADD munge.key /etc/munge/munge.key
RUN chown munge:munge /etc/munge/munge.key
RUN chmod go-rw /etc/munge/munge.key
ADD startup.sh /etc/profile.d/
RUN chmod +x /etc/profile.d/startup.sh
ENTRYPOINT . /etc/profile.d/startup.sh && /bin/bash


