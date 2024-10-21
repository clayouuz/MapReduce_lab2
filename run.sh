mvn clean package
$HADOOP_HOME/sbin/start-dfs.sh


alias hls='hdfs dfs -ls'
alias hmkdir='hdfs dfs -mkdir'
alias hrm='hdfs dfs -rm'
alias hcat='hdfs dfs -cat'
alias hput='hdfs dfs -put'
alias hget='hdfs dfs -get'

hmkdir /user
hmkdir /user/clay/
hmkdir /user/clay/input

hput input/user_balance_table.csv input/dailyFlow
hput input/user_balance_table.csv input/userActivity
hput input/mfd_day_share_interest.csv input/transactionInfluence

hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main dailyFlow input/dailyFlow output/dailyFlow
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main weeklyFlow output/dailyFlow output/weeklyFlow
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main userActivity input/userActivity output/userActivity/raw
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main sortByActivateDays output/userActivity/raw output/userActivity/sorted
hadoop jar target/lab2-1.0-SNAPSHOT.jar com.example.Main transactionInfluence input/transactionInfluence output/transactionInfluence
hget output output
hrm -r output
$HADOOP_HOME/sbin/stop-dfs.sh