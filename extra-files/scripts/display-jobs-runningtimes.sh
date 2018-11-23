#!/bin/bash

echo "`grep "Running job " $1/main_log`"

started_jobs=`grep "Running job\:" $1/main_log | wc -l`
finished_jobs=`grep "completed successfully" $1/main_log | wc -l`

if [ "$started_jobs" != "$finished_jobs" ]; then
   echo "WARNING!! NOT ALL JOBS ARE DONE!!"
fi

echo "Started jobs $started_jobs finished jobs $finished_jobs"

echo "-------------------------------------------------"
starting_jobs=`grep "Running job\:" $1/main_log`
`grep "completed successfully" $1/main_log > tmp`

rm $1/job_runningtimes

while read -r job; do
   day=`echo $job | awk '{print $1}'`
   time=`echo $job | awk '{print $2}'`
   job_id=`echo $job | awk '{print $7}'`
   time_start=`date -d "20$day $time " +%s`
   #echo "$job_id start day $day time $time epoc $time_start"

   job_finish=`grep $job_id tmp`
   if [ "$job_finish" ] ; then
       day=`echo $job_finish | awk '{print $1}'`
       time=`echo $job_finish | awk '{print $2}'`
       time_end=`date -d "20$day $time " +%s`
       #echo "$job_id finish day $day time $time epoc $time_end"
   
       echo "$job_id $(( $time_end - $time_start ))" >> $1/job_runningtimes
   fi
done <<< "$starting_jobs"
echo "-------------------------------------------------"

#grep "Running job\:\|completed successfully" $1/main_log | sed -e 's/Running //g' | sort -k 6 | awk '{key=$1" "$2; getline; print key " " $1 " " $2 " " $6;}' | awk '{print $2,$4,$5}' | tr ':' ' ' | awk '{print($7,($6+$5*60+$4*60*60)-($3+$2*60+$1*60*60)," seconds")}'
