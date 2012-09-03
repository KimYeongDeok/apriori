#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/etl_data_small/"
OUTPUT="youngdeok/etl_generate/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="generate"
PARAMETER="sequence"

ssh $UCLOUD hadoop fs -rmr $OUTPUT
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -parameter $PARAMETER
ssh $UCLOUD hadoop fs -tail $OUTPUT/part-m-00014
ssh $UCLOUD hadoop fs -tail $OUTPUT/part-m-00015
