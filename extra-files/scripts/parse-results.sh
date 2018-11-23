#!/bin/bash

for file in container*syslog*; do
	task=`grep done $file  | tail -n 1 | grep -o 'attempt_[0-9_mr]*'`
	st_time="`head -n 1 $file | awk '{print $1,$2}'`"
	st_time1970=$(date -d "$st_time" "+%s")

	e_time=`tail -n 1 $file | awk '{print $1,$2}'`
	e_time1970=$(date -d "$e_time" "+%s")
	

	st_time_proc=$(date -d "`grep -m 1 "Processing split" $file| awk '{print $1,$2}'`" "+%s")
	dur=$(($e_time1970 - $st_time1970))
	
	echo $file $task $dur `echo $st_time | awk '{print $2}'`  `echo $e_time | awk '{print $2}'`   | sed 's/ /   /g'
	#echo $file $task $dur $st_time1970 `echo $st_time | awk '{print $2}'` `echo $e_time | awk '{print $2}'` | sed 's/ /   /g'
done

echo
echo
echo "toPreempt"
grep -rn "toPreempt" yarn-root-resourcemanager* | grep -v "toPreempt size: 0$"
echo "to suspend"
grep " to suspend"  yarn-root-resourcemanager* | grep Policy | awk '{print $1,$2,$7}'
echo
grep "qT.toBePreempted" yarn-root-resourcemanager* | sed 's/INFO Prop.*y: PAMELA//g' | grep -v  '<memory:0, vCores:0>'

exit
grep "try to preempt"  yarn-root-resourcemanager*

