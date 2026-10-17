pgrep -fo atalanta-rs.jar | xargs kill
cd ../core
./start-core.sh
cd ../rs
nohup java -Xmx250m -Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=8340 \
-Dcom.sun.management.jmxremote.host=127.0.0.1 \
-Dcom.sun.management.jmxremote.rmi.port=8340 \
-Dcom.sun.management.jmxremote.ssl=false \
-Dcom.sun.management.jmxremote.registry.ssl=false \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.local.only=true \
-Djava.rmi.server.hostname=127.0.0.1 \
-jar atalanta-rs.jar >/dev/null 2>&1 &
pgrep -fo atalanta-rs.jar > ./atalanta-rs.pid