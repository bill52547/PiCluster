FROM 192.168.1.133:5000/spack
ENV SLURM_CONF=/etc/slurm-llnl/slurm.conf
RUN useradd munge && useradd slurm && mkdir /tmp/slurm && mkdir /tmp/slurm/slurmctld/ && chown slurm:slurm -R /tmp/slurm
ADD slurm.conf /etc/slurm-llnl/slurm.conf
ADD munge.key /etc/munge/munge.key
RUN chown munge:munge /etc/munge/munge.key && \
    chmod go-rwx /etc/munge/munge.key && \
    mkdir /tmp/munge/ && chown munge:munge -R /tmp/munge 
ADD startup.sh /etc/profile.d/


