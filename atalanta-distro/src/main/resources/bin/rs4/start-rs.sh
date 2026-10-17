#!/bin/bash

ps ax | grep adb | grep atalanta_r4.prop | grep -v grep| awk '{print $1}' | xargs kill
cd ../core
nohup ./adb atalanta_r4.prop 2>&1 | tee -a ../../logs/core4.log &
cd ../rs4
java -Xmx250m -Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=8344 \
-Dcom.sun.management.jmxremote.host=127.0.0.1 \
-Dcom.sun.management.jmxremote.rmi.port=8344 \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.registry.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.local.only=true \
-Djava.rmi.server.hostname=127.0.0.1 \
-Dspring.profiles.active=%env_profile% \
-jar atalanta-rs.jar --server.port=8306 rs4 &
