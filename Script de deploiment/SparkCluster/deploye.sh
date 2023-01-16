#!/bin/bash

wget https://dlcdn.apache.org/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
tar -xzf spark-3.3.1-bin-hadoop3.tgz
mkdir Deploye
chmod 777 Deploye/
mv spark-3.3.1-bin-hadoop3 Deploye
export SPARK_HOME=$HOME/Deploye/spark-3.3.1-bin-hadoop3
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
source $HOME/.bashrc
cd Deploye/spark-3.3.1-bin-hadoop3/sbin
./start-master.sh
./start-worker.sh spark://pop-os.localdomain:7077
cd ..
cd bin
spark-submit --master spark://pop-os.localdomain:7077 --conf spark.app.name="lolo" --num-executors 2 --executor-cores 1 --executor-memory 1G /home/lex__/IdeaProjects/spar_1/target/spar_1-1.0-SNAPSHOT.jar

