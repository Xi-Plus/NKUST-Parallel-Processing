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

run:
	hadoop com.sun.tools.javac.Main KMeans.java
	jar cf KMeans.jar KMeans*.class
	hadoop fs -rm -R /user/xiplus/KMeans/output* || true
	hadoop jar KMeans.jar KMeans /user/xiplus/KMeans/input /user/xiplus/KMeans/output

ls:
	hadoop fs -ls -R /user/xiplus/KMeans

cat:
	hadoop fs -cat /user/xiplus/KMeans/output0/part-00000

push:
	hadoop fs -rm /user/xiplus/KMeans/input/centroid.txt || true
	hadoop fs -rm /user/xiplus/KMeans/input/computers.csv || true
	hadoop fs -copyFromLocal /home/xiplus/homework/centroid.txt /user/xiplus/KMeans/input
	hadoop fs -copyFromLocal /home/xiplus/homework/computers.csv /user/xiplus/KMeans/input
	hadoop fs -ls /user/xiplus/KMeans/input

pull:
	rm -R output* || true
	hadoop fs -get /user/xiplus/KMeans/output* .
