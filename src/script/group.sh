#!/bin/sh

UCLOUD="root@14.63.225.83"
#INPUT="hyunje/apat.txt"
INPUT="youngdeok/etl_group_data"
OUTPUT="youngdeok/etl_group/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="group"
PARAMETER="0,1"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -parameter $PARAMETER
ssh $UCLOUD hadoop fs -tail $OUTPUT/part-r-00000
ssh $UCLOUD hadoop fs -tail $OUTPUT/part-r-00001