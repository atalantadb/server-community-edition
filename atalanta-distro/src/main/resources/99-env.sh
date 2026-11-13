#!/bin/bash

# set by yourself
export ZK_HOME="/zookeeper/bin"

# do not touch
export ATALANTA_HOME=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
export ATALANTA_BIN=$ATALANTA_HOME/bin
export ATALANTA_SCRIPTS=$ATALANTA_HOME/scripts

# log
echo "Environment variables were set!"
