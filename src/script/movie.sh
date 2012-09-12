#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/movie/ratings_shot.dat"
OUTPUT="youngdeok/movie/output"
MAPREDUCE="FrontDriver"
DELIMITER="::"
COMMAND="apriori"
LEVEL="2"
SUPPORT="2"
LIB="-libjars /root/youngdeok/mysql-connector-java-5.1.21.jar,/root/youngdeok/guava-13.0.1.jar,/root/youngdeok/commons-pool-1.6.jar"
JAR="/root/youngdeok/hadoop-example.jar"

ssh $UCLOUD hadoop fs -rmr $OUTPUT*
ssh $UCLOUD hadoop jar $JAR org.openflamingo.hadoop.mapreduce.$MAPREDUCE $LIB -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -level $LEVEL -support $SUPPORT
ssh $UCLOUD hadoop fs -cat $OUTPUT*/part*
