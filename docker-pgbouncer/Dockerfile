FROM debian:buster-slim

COPY install-packages.sh .
RUN ./install-packages.sh
RUN useradd -ms /bin/bash pgbouncer
USER pgbouncer
WORKDIR /home/pgbouncer
