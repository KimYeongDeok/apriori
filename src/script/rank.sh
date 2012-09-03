#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/etl_rank_data/"
OUTPUT="youngdeok/etl_rank/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="rank"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND
ssh $UCLOUD hadoop fs -cat $OUTPUT/part-*