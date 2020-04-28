start:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

stop:
	stop-dfs.sh
	stop-yarn.sh
	mr-jobhistory-daemon.sh stop historyserver

restart: stop start

clear:
	rm /usr/local/hadoop/hadoop_data/hdfs/namenode/current/ -R || true
	rm /usr/local/hadoop/hadoop_data/hdfs/datanode/current/ -R || true
	hadoop namenode -format
