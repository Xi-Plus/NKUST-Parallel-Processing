start:
	sudo service zookeeper start
	/usr/local/hbase/bin/start-hbase.sh

stop:
	/usr/local/hbase/bin/stop-hbase.sh
	sudo service zookeeper stop

restart: stop start

clear:
	sudo rm -rf /home/xiplus/hbase/
	sudo rm -rf /home/xiplus/zookeeper/
