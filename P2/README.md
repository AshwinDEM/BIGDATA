Follow the commands

mkdir folder

javac -classpath $(hadoop classpath) -d folder M1.java R1.java Driver.java

jar -cvf Temp.jar -C folder/ .

hdfs dfs -mkdir input

hdfs dfs -put dataset124.csv /input/data.csv

hadoop jar Temp.jar Driver /input /output

hdfs dfs -ls /output

hdfs dfs -cat /output/part-r-00000
