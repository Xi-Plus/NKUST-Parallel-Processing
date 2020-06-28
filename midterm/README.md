```
javac -classpath /usr/local/hadoop/share/hadoop/common/hadoop-common-2.10.0.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.10.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.0.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/model/User.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.10.0.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.10.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.0.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/hbase/UsersDAO.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.10.0.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.10.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.0.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/UsersTool.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.10.0.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.10.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.0.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/model/Twit.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.10.0.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.10.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.0.jar:/usr/local/hbase/lib/* -d . ~/midterm/utils/Md5Utils.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.10.0.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.10.0.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.10.0.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/hbase/TwitsDAO.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.8.4.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.8.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.4.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/LoadUsers.java ~/midterm/utils/LoadUtils.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.8.4.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.8.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.4.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/LoadTwits.java

javac -classpath /home/xiplus/midterm/:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.8.4.jar:/usr/local/hadoop/share/hadoop/common/lib/hadoop-annotations-2.8.4.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.4.jar:/usr/local/hbase/lib/* -d . ~/midterm/HBaseIA/TwitBase/mapreduce/CountShakespeare.java

ls -lR HBaseIA

ls -lR utils

jar -cvf HBaseIA.jar -C HBaseIA .

jar -cvf utils.jar -C utils .

java -classpath /home/xiplus/midterm/:`hbase classpath` HBaseIA.TwitBase.UsersTool help

java -classpath /home/xiplus/midterm/:`hbase classpath` HBaseIA.TwitBase.LoadUsers help
java -classpath /home/xiplus/midterm/:`hbase classpath` HBaseIA.TwitBase.LoadTwits run
java -classpath /home/xiplus/midterm/:`hbase classpath` HBaseIA.TwitBase.mapreduce.CountShakespeare

```
