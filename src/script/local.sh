#!/bin/sh

UCLOUD="localhost"
INPUT="youngdeok/apriori/apriori.data"
OUTPUT="youngdeok/apriori/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="apriori"
LEVEL="2"
LIB="-libjars /home/yd/.m2/repository/mysql/mysql-connector-java/5.1.21/mysql-connector-java-5.1.21.jar,/home/yd/.m2/repository/com/google/guava/guava/13.0.1/guava-13.0.1.jar"

hadoop fs -rmr $OUTPUT*
hadoop jar /home/yd/workspace/hadoop/apriori/target/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE $LIB -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -level $LEVEL
hadoop fs -cat $OUTPUT*/part*
