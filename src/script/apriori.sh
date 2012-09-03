#!/bin/sh

UCLOUD="root@14.63.225.83"
INPUT="youngdeok/apriori/apriori.data"
OUTPUT="youngdeok/apriori/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="apriori"
PARAMETER=""

ssh $UCLOUD hadoop fs -rmr $OUTPUT*
ssh $UCLOUD hadoop jar /root/youngdeok/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND
ssh $UCLOUD hadoop fs -cat $OUTPUT*/part-*
