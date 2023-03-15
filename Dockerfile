FROM ubuntu:latest

LABEL maintainer="Hana Bouacila <ih_bouacila@esi.dz>, Yousra Fantazi <iy_fantazi@esi.dz>"

#configure the work directory 
WORKDIR /root


# Update packages and install necessary tools : install openssh-server, openjdk and wget, vim, python 
RUN apt-get update && apt-get -y upgrade && \
    apt-get -y install  openssh-server  wget vim openjdk-8-jdk 
    #apt-get install -y python3   python3-pip && \
    #apt install -y python3.10-venv && \
    #python3 -m venv myenv && \
    #source myenv/bin/activate && \
    #pip install -y --upgrade urllib3 chardet 
    

   



# install hadoop 3.3.4
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz && \
    tar -xzf hadoop-3.3.4.tar.gz && \
    mv hadoop-3.3.4 /usr/local/hadoop && \
    rm hadoop-3.3.4.tar.gz

#RUN groupadd hdfs && \
   # useradd -g hdfs hdfs

# environment variables for hadoop
ENV HADOOP_HOME=/usr/local/hadoop 
ENV HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
ENV LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH 


# install spark 3.3.2
RUN wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xvf spark-3.3.2-bin-hadoop3.tgz && \
    mv spark-3.3.2-bin-hadoop3 /usr/local/spark && \
    rm spark-3.3.2-bin-hadoop3.tgz



# environment variables for sprak 
ENV SPARK_HOME=/usr/local/spark

# Install Delta Lake 2.2.0
RUN wget -O delta-core_2.12-2.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar && \
    mv delta-core_2.12-2.2.0.jar $SPARK_HOME/jars/ 
    # pyspark --packages io.delta:delta-core_2.12:2.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

#install pyspark
#RUN pip install pyspark

# Add Delta Lake configuration to Spark
RUN echo "spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_HOME/conf/spark-defaults.conf 

# set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 
#ENV PYSPARK_PYTHON=/usr/bin/python3

#maybe here, should add deltalake as well, plus the delta_home maybe 
ENV PATH=$PATH:/usr/local/hadoop/bin:/usr/local/hadoop/sbin:/usr/local/spark/bin


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
    mv /tmp/slaves $HADOOP_HOME/etc/hadoop/slaves && \
    mv /tmp/start-hadoop.sh ~/start-hadoop.sh && \
    mv /tmp/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf 

#Maybe here i should add the start container.sh and resize.sh 

RUN chmod +x ~/start-hadoop.sh && \
    chmod +x start-container.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh 

# format namenode #what is this ??
RUN /usr/local/hadoop/bin/hdfs namenode -format

CMD [ "sh", "-c", "service ssh start; bash"]


