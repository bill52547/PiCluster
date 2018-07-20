#!/bin/bash
echo Runing startup script...
#. $SPACK_ROOT/share/spack/setup-env.sh
/etc/init.d/munge restart
/etc/init.d/slurmctld restart

#spack load slurm munge htop
#export slurmctld=`spack location --install-dir slurm`/sbin/slurmctld 
#export munged=`spack location --install-dir slurm`/sbin/munged 
