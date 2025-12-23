#!/bin/sh

#
# Wojciech Golab, 2016-2019
#

if [ -f /usr/lib/jvm/default-java/bin/java ]; then
    JAVA_HOME=/usr/lib/jvm/default-java
elif [ -f /usr/lib/jvm/java-11-openjdk-amd64/bin/java ]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-8-openjdk-amd64/bin/java ]; then
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
elif [ -f /usr/lib/jvm/java-openjdk/bin/java ]; then
    JAVA_HOME=/usr/lib/jvm/java-openjdk
else
    echo "Unable to find java :("
    exit 1
fi

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 host port input_number"
    exit 1
fi

JAVA=$JAVA_HOME/bin/java
$JAVA -cp .:gen-java/:"lib-java/*" JavaClient $1 $2 $3
