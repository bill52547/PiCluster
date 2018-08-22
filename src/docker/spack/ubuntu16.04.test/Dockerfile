FROM ubuntu:16.04
RUN apt-get update && apt-get install -y python build-essential gfortran
ADD startup.sh /etc/profile.d
ENTRYPOINT . /etc/profile.d/startup.sh && /bin/bash
