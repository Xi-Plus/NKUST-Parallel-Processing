Commands:
```
hadoop com.sun.tools.javac.Main KMeans.java
	jar cf KMeans.jar KMeans*.class

hadoop fs -mkdir -p /user/xiplus/KMeans/input
hadoop fs -copyFromLocal /home/xiplus/homework/centroid.txt /user/xiplus/KMeans/input
hadoop fs -copyFromLocal /home/xiplus/homework/computers.csv /user/xiplus/KMeans/input
hadoop fs -copyFromLocal /home/xiplus/homework/small.csv /user/xiplus/KMeans/input
hadoop fs -ls /user/xiplus/KMeans/input

hadoop fs -rm -R /user/xiplus/KMeans/output*

hadoop jar KMeans.jar KMeans /user/xiplus/KMeans/input /user/xiplus/KMeans/output

hadoop fs -ls -R /user/xiplus/KMeans
hadoop fs -cat /user/xiplus/KMeans/output/part-r-00000

hadoop fs -rm /user/xiplus/KMeans/input/centroid.txt
```
