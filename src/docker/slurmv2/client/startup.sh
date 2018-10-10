# /bin/sh
echo Runing startup script...
#. $SPACK_ROOT/share/spack/setup-env.sh
/etc/init.d/munge restart
#echo Spin up Gate7.2...
#./opt/spack/share/spack/setup-env.sh
#source <(spack module loads --dependencies gate@7.2)
#source geant4.sh
#spack load slurm munge htop
#export slurmctld=`spack location --install-dir slurm`/sbin/slurmctld 
#export munged=`spack location --install-dir slurm`/sbin/munged 
