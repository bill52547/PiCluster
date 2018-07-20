#!/bin/bash
sudo proxychains docker run -v /home/hongxwing/Workspace/spack/app/spack:/spack -it --rm 192.168.1.133:5000/spack:0.0.3
