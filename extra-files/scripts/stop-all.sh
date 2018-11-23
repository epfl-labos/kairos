#!/bin/bash

echo "Stopping HDFS"
clush -w dco-node002 '/disk3/kairos/hadoop-2.7.1/sbin/hadoop-daemon.sh --script hdfs stop namenode'

clush -w @extra_kairos '/disk3/kairos/hadoop-2.7.1/sbin/hadoop-daemons.sh --script hdfs stop datanode'

echo "Stopping RM and nodemanager"
clush -w dco-node002 '/disk3/kairos/hadoop-2.7.1/sbin/yarn-daemon.sh stop resourcemanager'

clush -w @extra_kairos '/disk3/kairos/hadoop-2.7.1/sbin/yarn-daemons.sh stop nodemanager'

echo "Finished stopping all YARN"
