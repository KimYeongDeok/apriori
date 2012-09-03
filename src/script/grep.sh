#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/etl_data/"
OUTPUT="youngdeok/etl_grep/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="grep"
PARAMETER="둘리"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -parameter $PARAMETER
ssh $UCLOUD hadoop fs -cat $OUTPUT/part-*

