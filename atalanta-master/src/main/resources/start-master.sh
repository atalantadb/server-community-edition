pgrep -fo atalanta-master.jar | xargs kill
nohup java -Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=1199 \
-Dcom.sun.management.jmxremote.host=127.0.0.1 \
-Dcom.sun.management.jmxremote.rmi.port=1199 \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.registry.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.local.only=true \
-Djava.rmi.server.hostname=127.0.0.1 \
-Datalanta.config=./config/master-configuration.yml \
-cp ./atalanta-master.jar:./config \
com.light.atalanta.master.Application >/dev/null 2>&1 &
pgrep -fo atalanta-master.jar > ./atalanta-master.pid