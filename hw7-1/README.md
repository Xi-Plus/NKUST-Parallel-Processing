Commands:
```
hadoop com.sun.tools.javac.Main hw71.java
jar cf hw71.jar hw71*.class

hadoop fs -mkdir -p /user/xiplus/hw71/input
hadoop fs -copyFromLocal /home/xiplus/homework/7_HW_input.txt /user/xiplus/hw71/input
hadoop fs -ls /user/xiplus/hw71/input

hadoop fs -rm -R /user/xiplus/hw71/output

hadoop jar hw71.jar hw71 /user/xiplus/hw71/input /user/xiplus/hw71/output

hadoop fs -ls -R /user/xiplus/hw71
hadoop fs -cat /user/xiplus/hw71/output/part-r-00000
```
