mvn clean package
$HADOOP_HOME/sbin/start-dfs.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/clay/
hdfs dfs -mkdir /user/clay/input
hdfs dfs -put input/* input/

alias hls='hdfs dfs -ls'
alias hmkdir='hdfs dfs -mkdir'
alias hrm='hdfs dfs -rm'
alias hcat='hdfs dfs -cat'
alias hput='hdfs dfs -put'
alias hget='hdfs dfs -get'

hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main dailyFlow input/dailyFlow output/dailyFlow
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main weeklyFlow output/dailyFlow output/weeklyFlow
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main userActivity input/userActivity output/userActivity/raw
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main sortByActivateDays output/userActivity/raw output/userActivity/sorted
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main transactionInfluence input/transactionInfluence output/transactionInfluence

hdfs dfs -rm -r output
$HADOOP_HOME/sbin/stop-dfs.sh