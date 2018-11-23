#!/bin/bash

      MAIN_DIR="/disk3/kairos/"
      JAR_FILE="$MAIN_DIR/hadoop-2.7.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0-florin.jar"
HDFS_INOUT_DIR="hdfs://dco-node002-10g:9000/"
       EXP_DIR="/disk3/exp-kairos"
      MAIN_LOG=$MAIN_DIR/main_log
     ALL_NODES=`clush -w @extra_kairos hostname 2>&1 | grep -v Ubuntu | awk '{print $2}' | sort`

CTR=$1
flopsMAP=0 #$2
flopsREDUCE=0 #$3

./stop-all.sh
#./kill-hadoop.sh
./clean-logs-all.sh
./start-all.sh
hadoop-2.7.1/bin/hdfs dfsadmin -safemode leave

cd $MAIN_DIR/hadoop-2.7.1/etc/hadoop 
clush -w @extra_kairos_slaves --copy yarn-site.xml
clush -w @extra_kairos_slaves --copy core-site.xml
clush -w @extra_kairos_slaves --copy capacity-scheduler.xml
clush -w @extra_kairos_slaves --copy mapred-site.xml
cd -
clush -w @extra_kairos_slaves --copy top-log.sh

clush -w @extra_kairos 'ps -ef | grep top | grep -v grep | awk '\''{print $2}'\''| xargs kill -9'
clush -w @extra_kairos 'cd /disk3/kairos;  ./top-log.sh </dev/null >top-stdout 2>&1 &'

sleep 5

single_run () {
	$MAIN_DIR/hadoop-2.7.1/bin/hadoop --config $MAIN_DIR/hadoop-2.7.1/etc/hadoop jar $JAR_FILE wordcountflorin \
		-D mapreduce.job.reduces=$1 \
		-D mapreduce.job.queuename=$2 \
		-D mapreduce.map.memory.mb=5120 \
		-D mapreduce.reduce.memory.mb=5120 \
		-D mapreduce.task.io.sort.mb=2000 \
		-D mapreduce.map.java.opts="-Xms5120m -Xmx5120m" \
		-D mapreduce.reduce.java.opts="-Xms5120m -Xmx5120m" \
		-D mapred.map.tasks.speculative.execution=false \
		-D mapred.reduce.tasks.speculative.execution=false \
		-D mapreduce.job.reduce.slowstart.completedmaps=1 \
		-D mapreduce.reduce.input.buffer.percent=1 \
		$HDFS_INOUT_DIR/$3 $HDFS_INOUT_DIR/$4 $flopsMAP $flopsREDUCE
}




hadoop-2.7.1/bin/hdfs dfs -rmr /user/root/HiBench/Terasort/Output*
hadoop-2.7.1/bin/hdfs dfs -rmr /Output*
hadoop-2.7.1/bin/hdfs dfs -rmr /terasort.out*
hadoop-2.7.1/bin/hdfs dfs -rmr /wordcount.*.out*


{
echo "Starting jobs at: `date` from workload file: $2"


echo "Starting jobs at: `date` from workload file: $2"

i=0
while read jobSpecs; do
   i=$(($i+1))
   echo "Running job $jobSpecs i $i"
   sleepVar=$(echo $jobSpecs | cut -f1 -d' ')
   sleep $sleepVar

   nrTasksVar=$(echo $jobSpecs | cut -f2 -d' ')
   queueVar=$(echo $jobSpecs | cut -f3 -d' ')
   inputVar=$(echo $jobSpecs | cut -f4 -d' ')
   outputVar="$(echo $jobSpecs | cut -f5 -d' ')$i"
   echo "Output directory $outputVar"
   extraFlopsMapVar=$(echo $jobSpecs | cut -f6 -d' ')
   extraFlopsReduceVar=$(echo $jobSpecs | cut -f7 -d' ')
   
   flopsMAP=$extraFlopsMapVar
   flopsREDUCE=$extraFlopsReduceVar
   single_run $nrTasksVar $queueVar $inputVar $outputVar &
done <$2

#############################
#flopsMAP=500
#flopsREDUCE=500
#single_run 15   "long" "/random-100char-strings"           "wordcount.out-30GB"     &
#sleep 4 
#flopsMAP=50
#flopsREDUCE=50
#single_run 7   "short" "/random-100char-small"              "wordcount.out-4.5GB-1"   &
#sleep 4
#flopsMAP=500
#flopsREDUCE=500
#single_run 15   "short" "/random-100char-small2"             "wordcount.out-4.5GB-2"  &



#single_run 60   "default" "terasort.in-60GB-60blks"  	       "terasort.out-30GB.2"     &

#single_run 15  "default" "terasort.in-30GB-15blks"  	       "terasort.out-30GB.2"     &
#single_run 10  "default" "terasort.in-15GB-150blks-repl3"       "terasort.out-30GB.1"     &
#single_run 5   "default" "terasort.in-1.5GB-15blks-repl3"       "terasort.out-30GB.2"     &

wait
echo "Finished jobs at: `date`"
} >$MAIN_LOG  2>&1

hadoop-2.7.1/bin/hdfs dfs -rmr /wordcount.*.out*

clush -w @extra_kairos 'ps -ef | grep top | grep -v grep | awk '\''{print $2}'\''| xargs kill -9'

cd $EXP_DIR
NEW_DIR="`date +\"%Y-%m-%d_%H-%M-%S\"`--$CTR"
mkdir $NEW_DIR
cd $NEW_DIR


#copying logs
for node in $ALL_NODES; do 
	mkdir $node
	scp -r $node:$MAIN_DIR/hadoop-2.7.1/logs/* 	$node
	scp -r $node:$MAIN_DIR/logs/* 			$node
	scp -r $node:$MAIN_DIR/top-output* 		$node
done
find -type f -size 0 | xargs rm -fr

for file in `find -type f -name '*sys*'`; do
	mv $file `echo $file | tr "/" " " | awk '{print $5"_"$6"_"$2}'`
done
for file in `find -type f -name '*std*'`; do
	mv $file `echo $file | tr "/" " " | awk '{print $5"_"$6"_"$2}'`
done

find | grep userlogs | xargs rm -fr

for dir in dco*; do
	mv $dir/* .
done
find -type d -name 'dco*' | xargs rm -fr
find -type f -name '*.out' | xargs rm -fr 
cp $MAIN_LOG .
cp $MAIN_DIR/run-workload.sh .
cp $2 .
cp -r $MAIN_DIR/hadoop-2.7.1/etc/hadoop .
#cp  $MAIN_DIR/hadoop-2.7.1/etc/hadoop/yarn-site.xml .
#cp  $MAIN_DIR/hadoop-2.7.1/etc/hadoop/capacity-scheduler.xml .
#cp  $MAIN_DIR/hadoop-2.7.1/etc/hadoop/core-site.xml .
#cp  $MAIN_DIR/hadoop-2.7.1/etc/hadoop/mapred-site.xml .
#cp  $MAIN_DIR/hadoop-2.7.1/etc/hadoop/hdfs-site.xml .

/disk3/kairos/parse-results.sh   | sort -k2,2g > parsed_results

/disk3/kairos/display-jobs-runningtimes.sh . > job_running_times_output
exit

#aliging top logs ########################################################3


declare -a SEC_1970_ARR
declare -a IN_SEC_ARR

EARLIEST=4129647925
LATEST=0
MONITORING_INTERVAL=0.1

NODE_LIST=`ls | grep top | grep -o [0-9]*$ | sort | uniq`

for node in $NODE_LIST; do

	for file in top-output-0001_*$node; do
        	FIRST_TSTAMP=`head -n 1 $file | awk '{print $1}'`
		LAST_TSTAMP=`tail -n 1 $file | awk '{print $1}'`
	        SEC_1970=`date --date=$FIRST_TSTAMP +"%s"`
		SEC_1970_E=`date --date=$LAST_TSTAMP +"%s"`
       		if [ $SEC_1970 -lt $EARLIEST ]; then
                	EARLIEST=$SEC_1970
       		fi  
		
	        if [ $SEC_1970_E -gt $LATEST ]; then
        	        LATEST=$SEC_1970_E
       		 fi

	done	

	EARLIEST=$(($EARLIEST-1))
	EXPECTED_TIMES_FIRST=`echo $MONITORING_INTERVAL | awk '{print int(1/$1)}'`

	for file in top-output-0001_*$node; do
        	FIRST_TSTAMP=`head -n 1 $file | awk '{print $1}'`
		LAST_TSTAMP=`tail -n 1 $file | awk '{print $1}'`
	        SEC_1970=`date --date=$FIRST_TSTAMP +"%s"`
		SEC_1970_E=`date --date=$LAST_TSTAMP +"%s"`
        	TIMES_FIRST=`head -n 20 $file | awk '{print $1}' | uniq -c | awk '{print $1}'`  
	        PADDING=`echo $SEC_1970 $EARLIEST $EXPECTED_TIMES_FIRST $TIMES_FIRST | awk '{print ($1-$2-1)*$3+$3-$4}'`
       		echo $file $PADDING
	        for i in `seq 1 $PADDING`; do
			echo "--:--:-- --" >> $file.ttmp
	        done    
		PADDING_END=`echo $LATEST $SEC_1970_E $EXPECTED_TIMES_FIRST | awk '{print ($1-$2)*$3}'`
	        for i in `seq 1 $PADDING_END`; do
			echo "--:--:-- --" >> $file.ttmp
	        done    
            
        	cat $file >> $file.ttmp
	done

	paste *ttmp | sed 's/^[^0-9]*$//g' | cat -s > $node.all 
	rm *ttmp

done

#end of aliging top logs ######################################################

grep 'UPDATE END' *  > updates_all

sed 's/\(yarn.*log\).*0001_01_0*\([0-9_]*\).*millis \([0-9]*\) .*/\1 \2 \3/g' updates_all | sort -k2,2g | awk '{if ($2>31 && $3 > 80 ){print $0}}' | sort -k3,3g > docker-update-overheads-mappers
sed 's/\(yarn.*log\).*0001_01_0*\([0-9_]*\).*millis \([0-9]*\) .*/\1 \2 \3/g' updates_all | sort -k2,2g | awk '{if ($2<=31 && $3 > 80 ){print $0}}' | sort -k3,3g > docker-update-overheads-reducers

