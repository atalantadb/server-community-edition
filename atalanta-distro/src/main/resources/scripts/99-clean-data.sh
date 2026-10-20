#!/bin/bash

# set environment variables
source ../99-env.sh

# delete ZooKeeper node
cd $ZK_HOME
echo XXX $(pwd)
echo "deleteall /atalanta" | ./zkCli.sh

# delete data dirs
cd $ATALANTA_HOME
rm -r data/rs1/*
rm -r data/rs2/*
rm -r data/rs3/*
rm -r data/rs4/*

# create data dirs
cd $ATALANTA_HOME
mkdir -p data/rs1
mkdir -p data/rs2
mkdir -p data/rs3
mkdir -p data/rs4

echo ALL DATA REMOVED!
