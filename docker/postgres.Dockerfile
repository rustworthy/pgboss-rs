FROM postgres:16.4

COPY certs /var/lib/postgresql/certs
RUN chmod 600 /var/lib/postgresql/certs/*
RUN chown -R postgres:postgres /var/lib/postgresql/certs