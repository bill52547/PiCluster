FROM 192.168.1.133:5000/nis-client
RUN apt-get -y install slurm-llnl
ADD munge.key /etc/munge/munge.key
RUN chown munge:munge /etc/munge/munge.key
RUN chmod go-rw /etc/munge/munge.key
ADD startup.sh /etc/profile.d/
RUN chmod +x /etc/profile.d/startup.sh


