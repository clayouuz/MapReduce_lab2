mvn clean package
$HADOOP_HOME/sbin/start-dfs.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/clay/
hdfs dfs -mkdir /user/clay/input
hdfs dfs -put input/* input/

hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main dailyFlow input/dailyFlow output/dailyFlow
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main weeklyFlow output/dailyFlow output/weeklyFlow
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main userActivity input/userActivity output/userActivity
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main transactionInfluence input/transactionInfluence output/transactionInfluence

hdfs dfs -rm -r output
$HADOOP_HOME/sbin/stop-dfs.sh