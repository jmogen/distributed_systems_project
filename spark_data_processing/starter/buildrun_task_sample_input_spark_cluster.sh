#!/bin/bash

# Build and run Spark tasks on cluster with sample input
# Spark Data Processing Benchmark

hostname | grep ecehadoop
if [ $? -eq 1 ]; then
    echo "This script must be run on ecehadoop :("
    exit -1
fi

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export SCALA_HOME=/usr
export HADOOP_HOME=/opt/hadoop-latest/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop/
export SPARK_HOME=/opt/spark-latest/
MAIN_SPARK_JAR="`ls $SPARK_HOME/jars/spark-core*.jar`:`ls $SPARK_HOME/jars/spark-common*.jar`"
export CLASSPATH=".:$MAIN_SPARK_JAR"

TASK_ID="Task1" # update here

echo --- Deleting
rm *.jar
rm *.class

echo --- Compiling
$SCALA_HOME/bin/scalac -J-Xmx1g $TASK_ID.scala
if [ $? -ne 0 ]; then
    exit
fi

echo --- Jarring
$JAVA_HOME/bin/jar -cf $TASK_ID.jar $TASK_ID*.class

echo --- Running
INPUT=/user/${USER}/a2_inputs/
#INPUT=/a2_inputs/in0.txt # update here
OUTPUT=/user/${USER}/a2_starter_code_output_spark/

$HADOOP_HOME/bin/hdfs dfs -mkdir $INPUT
$HADOOP_HOME/bin/hdfs dfs -copyFromLocal sample_input/smalldata.txt $INPUT
$HADOOP_HOME/bin/hdfs dfs -rm -R $OUTPUT
time $SPARK_HOME/bin/spark-submit --master yarn --class $TASK_ID --driver-memory 4g --executor-memory 4g $TASK_ID.jar $INPUT $OUTPUT

export HADOOP_ROOT_LOGGER="WARN"
$HADOOP_HOME/bin/hdfs dfs -ls $OUTPUT
$HADOOP_HOME/bin/hdfs dfs -cat $OUTPUT/* | sort > normalized_output.txt
