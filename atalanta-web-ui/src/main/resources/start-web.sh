ps ax | grep atalanta-web-ui | grep -v grep | awk '{print $1}' | xargs kill
nohup java -jar atalanta-web-ui.jar --server.port=8333 --atalanta.config=../master/config/master-configuration.yml --atalanta.client.config=../shell/config/atalanta-site.xml &
pgrep -fo atalanta-web-ui.jar > ./atalanta-web-ui.pid