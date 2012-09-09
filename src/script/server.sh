#!/bin/sh

UCLOUD="ydserver@192.168.10.53"
INPUT="youngdeok/apriori/apriori.data"
OUTPUT="youngdeok/apriori/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="apriori"
LEVEL="2"

ssh $UCLOUD hadoop fs -rmr $OUTPUT*
scp workspace/hadoop/apriori/target/hadoop-example.jar $UCLOUD:/home/ydserver
ssh $UCLOUD hadoop jar hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -level $LEVEL
ssh $UCLOUD hadoop fs -cat $OUTPUT*/part*
