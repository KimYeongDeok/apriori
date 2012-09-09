#!/bin/sh

UCLOUD="localhost"
INPUT="youngdeok/apriori/apriori.data"
OUTPUT="youngdeok/apriori/output"
MAPREDUCE="FrontDriver"
DELIMITER=","
COMMAND="apriori"
LEVEL="2"

ssh $UCLOUD hadoop fs -rmr $OUTPUT*
ssh $UCLOUD hadoop jar workspace/hadoop/apriori/target/hadoop-example.jar org.openflamingo.hadoop.mapreduce.$MAPREDUCE -input $INPUT -output $OUTPUT -delimiter $DELIMITER -command $COMMAND -level $LEVEL
ssh $UCLOUD hadoop fs -cat $OUTPUT*/part*
