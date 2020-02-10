FROM ubuntu:20.04

RUN apt-get update && apt-get install \
	automake \
	build-essential \
	libtool \
	libperl-dev \
	pkg-config

CMD ["/bin/bash"]
