#!/bin/bash

source settings.sh

REMOTE_HOST=$3
FEHOST=$4
FEPORT=$5
BEPORT=$6
CORES=$7

#JAVA=/usr/lib/jvm/java-1.8.0/bin/java
#JAVA=/usr/java/jdk-11.0.2/bin/java
if [ -f /usr/lib/jvm/default-java/bin/javac ]; then
	JAVA_HOME=/usr/lib/jvm/default-java
elif [ -f /usr/lib/jvm/java-11-openjdk-amd64/bin/javac ]; then
	JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
elif [ -f /usr/lib/jvm/java-8-openjdk-amd64/bin/javac ]; then
	JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-openjdk/bin/javac ]; then
	JAVA_HOME=/usr/lib/jvm/java-openjdk
else
	echo "Unable to find java compiler :("
	exit 1
fi

JAVA=$JAVA_HOME/bin/java

# how long to wait before running command
TIMEA=$1
# how long to run command before sending SIGINT
TIMEB=$2
# hack to send SIGKILL one second after SIGINT
TIMEC=`expr $TIMEB + 1`
current_dir=$(pwd)

sleep $TIMEA
echo Executing $0
ssh $SSH_OPTS -t $REMOTE_HOST "cd $current_dir;ls -l;timeout -s SIGKILL $TIMEC bash -c 'timeout -s SIGINT $TIMEB taskset -c $CORES $JAVA -Xmx${HEAPGB}g -cp .:gen-java/:\"lib/*\":\"jBCrypt-0.4/*\" BENode $FEHOST $FEPORT $BEPORT &> benode.$REMOTE_HOST.$BEPORT.log'"
sleep 1
echo Done executing $0
