#!/bin/bash



SAVEIFS=$IFS
IFS=$(echo -en "\n\b")

LIST=`cat locate-output2`
for item in $LIST; do
	node=`echo $item | awk '{print $1}'`
	path=`echo $item | awk '{print $2}'`
	scp hadoop-mapreduce-client-core-2.4.0.jar $node:$path
done 

IFS=$SAVEIFS

