FROM postgres:18

WORKDIR /var/lib/postgresql/
COPY certs ./certs
COPY postgresql.conf .
RUN chmod 600 certs/* && chown -R postgres:postgres certs
