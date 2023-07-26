FROM ubuntu:latest

LABEL maintainer="Hana Bouacila <ih_bouacila@esi.dz>, Yousra Fantazi <iy_fantazi@esi.dz>"

#configure the work directory 
WORKDIR /root


# Update packages and install necessary tools : install openssh-server, openjdk and wget, vim, python 
RUN apt-get update && apt-get -y upgrade && \
    apt-get -y install  openssh-server  wget vim openjdk-8-jdk 
   
    
ENV SPARK_VERSION 3.3.2
ENV HADOOP_VERSION 3.3.4
ENV DELTA_VERSION 2.2.0
ENV SCALA_VERSION 2.1


# install hadoop 3.3.4
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz && \
    tar -xzf hadoop-3.3.4.tar.gz && \
    mv hadoop-3.3.4 /usr/local/hadoop && \
    rm hadoop-3.3.4.tar.gz



# environment variables for hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
#ENV PYSPARK_PYTHON=/usr/bin/python3
ENV HADOOP_HOME=/usr/local/hadoop 
ENV KAFKA_HOME=/usr/local/kafka
#maybe here, should add deltalake as well, plus the delta_home maybe 
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin:/usr/local/kafka/bin
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH 


# install spark 3.3.2
RUN wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xvf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3 /usr/local/spark && \
    rm spark-3.3.2-bin-hadoop3.tgz


# environment variables for sprak 
ENV SPARK_HOME=/usr/local/spark



# DeltaLake
#RUN wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar -P $SPARK_HOME/jars/ && \
   # wget https://repo1.maven.org/maven2/io/delta/delta-storage/$DELTA_VERSION/delta-storage-$DELTA_VERSION.jar -P $SPARK_HOME/jars/

# Install Delta Lake 2.2.0
RUN wget -O delta-core_2.12-2.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar && \
    mv delta-core_2.12-2.2.0.jar $SPARK_HOME/jars/ 
RUN wget -O delta-storage_2.12-2.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar  && \
    mv delta-storage_2.12-2.2.0.jar $SPARK_HOME/jars/

    
# install kafka
RUN wget  https://archive.apache.org/dist/kafka/3.4.0/kafka_2.12-3.4.0.tgz && \  
    tar -xzvf kafka_2.12-3.4.0.tgz && \
    mv kafka_2.12-3.4.0 /usr/local/kafka && \
    rm kafka_2.12-3.4.0.tgz





# ssh without key
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -P '' && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

#image dirs 
RUN mkdir -p ~/hdfs/namenode && \ 
    mkdir -p ~/hdfs/datanode && \
    mkdir $HADOOP_HOME/logs


#Here should create the config dir 
COPY config/* /tmp/


#need to add what suitable for delta lake 
RUN mv /tmp/ssh_config ~/.ssh/config && \
    mv /tmp/hadoop-env.sh /usr/local/hadoop/etc/hadoop/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml && \
    mv /tmp/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml && \
    mv /tmp/workers $HADOOP_HOME/etc/hadoop/workers && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh && \
    mv /tmp/run-wordcount.sh ~/run-wordcount.sh && \
    mv /tmp/start-kafka-zookeeper.sh ~/start-kafka-zookeeper.sh && \
    mv /tmp/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf && \
    mv /tmp/add-spark-jars-to-hdfs.sh ~/add-spark-jars-to-hdfs.sh && \
    mv /tmp/convertToDeltaBatch.scala ~/convertToDeltaBatch.scala && \
    mv /tmp/convertToDeltaStreaming.scala ~/convertToDeltaStreaming.scala && \
    mv /tmp/hdfs_copy_file.sh ~/hdfs_copy_file.sh && \
    #not built yet 
    mv /tmp/detectNulls.py ~/detectNulls.py



#Maybe here i should add the start container.sh and resize.sh 

RUN chmod +x ~/start-hadoop.sh && \
    chmod +x ~/run-wordcount.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x ~/start-kafka-zookeeper.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh && \
    chmod +x ~/add-spark-jars-to-hdfs.sh && \
    chmod +x ~/convertToDeltaBatch.scala && \
    chmod +x ~/convertToDeltaStreaming.scala && \
    chmod +x ~/hdfs_copy_file.sh
# format namenode 
RUN /usr/local/hadoop/bin/hdfs namenode -format

#scripts to start automatically everytime the container starts
CMD [ "sh", "-c", "service ssh start; bash"]


