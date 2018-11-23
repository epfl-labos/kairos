#!/bin/bash

rm top-output-*
declare -a ARR

NODE=`hostname | awk -F- '{print $2}'`

while [ 1 -gt 0 ];do
	
	PROC_LIST=`ps -ef | egrep _[rm]_ | grep -v bash | grep -v grep | grep -v vim | grep -v top | awk '{print $2}'`	
	for pid in $PROC_LIST;do
		if [ ${ARR[$pid]+_} ];then
			echo "$pid already in list" > /dev/null
		else
			ARR[$pid]=1
			TASK_ID=`ps -ef | egrep _[rm]_  | grep -v bash | grep -v grep | awk -vPID=$pid '{if ($2==PID) {print $0}}' | grep -o [0-9]*_[mr]_[0-9_]*`
			top -b -n2000 -d0.1 -p $pid | stdbuf -i0 -o0 -e0 grep java |  stdbuf -i0 -o0 -e0 gawk '{print strftime("%H:%M:%S", systime()),$9}' | stdbuf -i0 -o0 -e0 sed 's/\.[0-9]//g' > top-output-${TASK_ID}-$NODE &
			echo "Added $pid of task $TASK_ID on node $NODE at `date`"
			ps -ef | egrep _[rm]_  | grep -v bash | grep -v grep | grep $pid
			echo
		fi
	done
	sleep 0.5
done

