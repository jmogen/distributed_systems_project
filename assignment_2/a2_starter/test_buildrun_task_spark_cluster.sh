#!/bin/bash

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

echo --- Deleting
rm *.jar
rm *.class

for ID in $(seq 4 4)
do
  echo --- Compiling Task${ID}
  $SCALA_HOME/bin/scalac -J-Xmx1g Task${ID}.scala
  if [ $? -ne 0 ]; then
      exit
  fi
  
  echo --- Jarring Task${ID} 
  $JAVA_HOME/bin/jar -cf Task${ID}.jar Task${ID}*.class
done

echo --- Running

echo --- Removing previously generated output file
rm ${USER}_output.txt
echo "Task#, Input, Time(s), Correct (Yes - 1, No - 0)" > ${USER}_output.txt

for ID in $(seq 4 4)
do
  for i in $(seq 0 5);
  do
  	INPUT=/a2_inputs/in$i.txt
  	OUTPUT=/user/${USER}/a2_starter_code_output_spark/in$i
  	$HADOOP_HOME/bin/hdfs dfs -rm -R $OUTPUT
  	time $SPARK_HOME/bin/spark-submit --master yarn --class Task$ID --driver-memory 4g --executor-memory 4g Task$ID.jar $INPUT $OUTPUT 2>&1 | tee -a time_output.txt
    ELAPSED=$(tail -n 2 time_output.txt | head -n 1 | grep -o '[0-9]\+:[0-9.]\+')
    
    $HADOOP_HOME/bin/hdfs dfs -ls $OUTPUT
  	$HADOOP_HOME/bin/hdfs dfs -cat $OUTPUT/* | sort | sed 's/[[:space:]]*$//' > normalized_output_TASK_${ID}_in${i}.txt
    sed 's/[[:space:]]*$//' ../a2_output/t${ID}_in${i}.txt > ref_normalized_TASK_${ID}_in${i}.txt
    if diff -w normalized_output_TASK_${ID}_in${i}.txt ref_normalized_TASK_${ID}_in${i}.txt > /dev/null; then 
      echo TASK_$ID, in${i}, $ELAPSED, 1 >> ${USER}_output.txt
      rm normalized_output_TASK_${ID}_in${i}.txt
      rm ref_normalized_TASK_${ID}_in${i}.txt
      rm time_output.txt
    else
      echo TASK_$ID, in${i}, $ELAPSED, 0 >> ${USER}_output.txt
      echo "--- DIFF for TASK_$ID, in${i} ---"
      diff -w normalized_output_TASK_${ID}_in${i}.txt ref_normalized_TASK_${ID}_in${i}.txt
      rm normalized_output_TASK_${ID}_in${i}.txt
      rm ref_normalized_TASK_${ID}_in${i}.txt
      rm time_output.txt 
    fi
  done
done

cat ${USER}_output.txt

