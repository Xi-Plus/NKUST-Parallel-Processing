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

run:
	javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.8.4.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.8.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.4.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/mapreduce/CountShakespeare.java
	jar -cvf HBaseIA.jar -C HBaseIA .
	java -classpath /home/xiplus/midterm/:`hbase classpath` HBaseIA.TwitBase.mapreduce.CountShakespeare
