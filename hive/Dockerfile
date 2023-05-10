FROM hadoop-spark-delta:latest

USER root

# get sources
RUN apt-get install -y libpostgresql-jdbc-java
RUN wget https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz -P /usr/local/
RUN tar -xzf /usr/local/apache-hive-3.1.3-bin.tar.gz -C /usr/local/
RUN mv  /usr/local/apache-hive-3.1.3-bin /usr/local/hive
RUN rm -rf /usr/local/apache-hive-3.1.3*

# set environment variables
ENV HIVE_HOME /usr/local/hive
ENV PATH $HIVE_HOME/bin:$PATH


RUN cp /usr/share/java/postgresql-jdbc4.jar /usr/local/hive/lib/
ADD config/hive-site.xml $HIVE_HOME/conf/hive-site.xml
RUN echo "export HADOOP_HOME=/usr/local/hadoop" >> /usr/local/hive/bin/hive-config.sh
RUN export LANGUAGE=en_US.UTF-8