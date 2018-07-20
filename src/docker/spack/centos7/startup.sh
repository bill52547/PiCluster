#!/bin/bash
echo Runing startup script...
export SPACK_ROOT=/opt/spack/spack
export PATH=$SPACK_ROOT/bin:${PATH}
. $SPACK_ROOT/share/spack/setup-env.sh
