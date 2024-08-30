Follow the commands


mkdir folder

javac -classpath $(hadoop classpath) -d folder RecordCountMapper.java RecordCountReducer.java RecordCount.java

jar -cvf recordcount.jar -C folder/ .

hdfs dfs -mkdir input

hdfs dfs -put dataset124.csv /input/data.csv

hadoop jar recordcount.jar RecordCount /input /output

hdfs dfs -ls /output

hdfs dfs -cat /output/part-r-00000
