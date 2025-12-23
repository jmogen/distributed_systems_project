#!/bin/bash

# This assignment requires Java 11, so we need to install it if it's not already present.
#
# (In my opinion) installing to a known, fixed location like `~/.local/lib/jvm/<version>` is best.
#
# To avoid any name collisions, let's prefix the variables with `ECE454_`.

ECE454_VERSION=jdk-11.0.25+9
ECE454_URL=https://github.com/adoptium/temurin11-binaries/releases/download/jdk-11.0.25%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.25_9.tar.gz
ECE454_PATH=$HOME/.local/lib/jvm
ECE454_TMP=$HOME/.local/tmp

if [ ! -d $ECE454_PATH/$ECE454_VERSION ]
then
    echo "Installing $ECE454_VERSION to $ECE454_PATH/$ECE454_VERSION"

    mkdir -p $ECE454_PATH
    mkdir -p $ECE454_TMP

    TARBALL=$ECE454_TMP/$ECE454_VERSION.tar.gz

    curl -qL $ECE454_URL -o $TARBALL || {
        echo "Failed to download $ECE454_VERSION"

        exit 1
    }

    tar -xzf $TARBALL -C $ECE454_PATH || {
        echo "Failed to extract tarball"

        exit 1
    }
fi

if [ ! -f $ECE454_PATH/$ECE454_VERSION/bin/javac ]
then
    echo "Java compiler not found at $ECE454_PATH/$ECE454_VERSION/bin/javac"

    exit 1
fi

unset JAVA_TOOL_OPTIONS

JAVA_HOME=$ECE454_PATH/$ECE454_VERSION

THRIFT_CC=/opt/bin/thrift
ZKPATH=../zookeeper-3.4.13
