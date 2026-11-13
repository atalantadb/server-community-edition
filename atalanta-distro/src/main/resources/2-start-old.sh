#!/bin/bash

# set environment variables
source 99-env.sh

# stop DB
./0-stop.sh

# start master 1 & 2
cd $ATALANTA_BIN/master1
./start-master.sh > /dev/null 2>&1
sleep 1
cd $ATALANTA_BIN/master2
./start-master.sh > /dev/null 2>&1
sleep 3

# start rs 1, 2, 3 & 4
cd $ATALANTA_BIN/rs1
./start-rs.sh > /dev/null 2>&1
cd $ATALANTA_BIN/rs2
./start-rs.sh > /dev/null 2>&1
cd $ATALANTA_BIN/rs3
./start-rs.sh > /dev/null 2>&1
cd $ATALANTA_BIN/rs4
./start-rs.sh > /dev/null 2>&1

cd $ATALANTA_HOME

