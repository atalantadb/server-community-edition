#!/bin/bash

# set environment variables
source ../99-env.sh

# clean logs
cd $ATALANTA_HOME
rm -r logs
mkdir logs

echo ALL LOGS REMOVED!
