#!/bin/sh

UCLOUD="ydserver@61.43.139.103"

#INPUT="youngdeok/movie/ratings.dat"
#INPUT="youngdeok/movie/ratings_shot.dat"
#OUTPUT="youngdeok/movie/output"
INPUT="youngdeok/test/test.dat"
OUTPUT="youngdeok/test/output"
#INPUT="youngdeok/apriori/apriori.data"
#OUTPUT="youngdeok/apriori/output"

MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="apriori"
LEVEL="3"
SUPPORT="2"

LIB="-libjars /home/ydserver/mysql-connector-java-5.1.21.jar,/home/ydserver/guava-13.0.1.jar,/home/ydserver/commons-pool-1.6.jar"
TARGET_JAR="/home/yd/workspace/hadoop/apriori/target/hadoop-example.jar"
JAR="/home/ydserver/hadoop-example.jar"

ssh $UCLOUD hadoop fs -rmr $OUTPUT*
scp $TARGET_JAR $UCLOUD:/home/ydserver
ssh $UCLOUD hadoop jar $JAR org.openflamingo.hadoop.mapreduce.$MAPREDUCE $LIB -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -level $LEVEL -support $SUPPORT
ssh $UCLOUD hadoop fs -cat $OUTPUT*/part*
