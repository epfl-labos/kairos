#!/bin/bash

echo "Starting HDFS..."
clush -w dco-node002 '/disk3/kairos/hadoop-2.7.1/sbin/hadoop-daemon.sh --script hdfs start namenode'

clush -w @extra_kairos '/disk3/kairos/hadoop-2.7.1/sbin/hadoop-daemons.sh --script hdfs start datanode'
echo "Done starting HDFS"

echo "Starting RM and nodemanagers"
clush -w dco-node002 '/disk3/kairos/hadoop-2.7.1/sbin/yarn-daemon.sh start resourcemanager'

clush -w @extra_kairos '/disk3/kairos/hadoop-2.7.1/sbin/yarn-daemons.sh start nodemanager'
echo "Done starting RM and nodemanagers"
