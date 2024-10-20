$HADOOP_HOME/sbin/start-all.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/clay/
hdfs dfs -mkdir /user/clay/input
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main dailyFlow input output