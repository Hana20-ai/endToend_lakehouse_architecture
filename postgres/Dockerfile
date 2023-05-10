FROM postgres

USER root

RUN export LANGUAGE=en_US.UTF-8
ADD config/hive-schema-3.1.0.postgres.sql /hive/hive-schema-3.1.0.postgres.sql

COPY config/init-hive-db.sh /docker-entrypoint-initdb.d/init-user-db.sh