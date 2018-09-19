export SPACK_ROOT="/opt/spack"
export PATH=$SPACK_ROOT/bin:$PATH
. /opt/spack/share/spack/setup-env.sh
source <(spack module loads --dependencies gate@7.2)
source geant4.sh
