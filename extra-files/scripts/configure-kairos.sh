#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: ./this pswindow[sec] queuelenght outputdir"
    echo "Example:  ./this 50 4 hadoop-2.7.1/etc/hadoop/"
    exit
fi

pswindow=$1
queuelength=$(($2+4))
outputdir=$3

yes | cp template-config-files/* $outputdir


sed -i "s/##processorsharing-totalcontainers##/$queuelength/g" $outputdir/capacity-scheduler.xml
##processorsharing-totalcontainers##
echo "Using $queuelength as queue length and $1 as ps window"
sed  -i "s/##processorsharing-windowsec##/$1/g" $outputdir/yarn-site.xml
##processorsharing-windowsec##
