#!/bin/bash

java -Xmx250m \
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006 \
-Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=1198 \
-Dcom.sun.management.jmxremote.host=127.0.0.1 \
-Dcom.sun.management.jmxremote.rmi.port=1198 \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.registry.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.local.only=true \
-Djava.rmi.server.hostname=127.0.0.1 \
-Datalanta.config=./config/master-configuration.yml \
-Dspring.profiles.active=%env_profile% \
-Datalanta.config=./config/master-configuration.yml \
-jar atalanta-master.jar --server.port=8302 master2 &