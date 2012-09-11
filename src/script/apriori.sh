#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/movie/ratings_shot.dat"
OUTPUT="youngdeok/movie/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="apriori"
LEVEL="2"

ssh $UCLOUD hadoop fs -rmr $OUTPUT*
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -level $LEVEL
ssh $UCLOUD hadoop fs -cat $OUTPUT*/part*
