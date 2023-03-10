#!/bin/bash


wget wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
tar -xzf hadoop-3.3.4.tar.gz

wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
tar -xzf spark-3.3.1-bin-hadoop3.tgz

export HADOOP_HOME=$HOME/hadoop-3.3.4
export JAVA_HOME=/usr/lib/jvm/default-java
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export SPARK_HOME=$HOME/spark-3.3.1-bin-hadoop3
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$HADOOP_HOME/bin:$PATH
export PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
source $HOME/.bashrc

$HADOOP_HOME/sbin/start-yarn.sh
cd spark-3.3.1-bin-hadoop3/bin
./bin/spark-submit --master yarn --deploy-mode cluster --driver-memory 4g --executor-memory 2g --executor-cores 1 /home/gilles-virtual-machine/Project-med-spark/target/Project-med-spark-1.0-SNAPSHOT.jar
