# KAIROS setting up
===========================================================================

MADE FOR INTERNAL USE ONLY

## Prerequisites

---------------------------------------------------------------------------

## ssh setup

```
clush -w @kairos_slaves "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa"
clush -w @kairos_slaves "cat ~/.ssh/id_rsa.pub" > slaves-pub.tmp
```
Edit slaves-pub.tmp. Take out dco-nodexxx part.

```
cat slaves-pub.tmp >> .ssh/authorized_keys
clush -w @kairos_slaves "cat ~/.ssh/id_rsa.pub >> .ssh/authorized_keys"
clush -w @kairos_master "cat ~/.ssh/id_rsa.pub >> .ssh/authorized_keys"
```

---------------------------------------------------------------------------

## setup docker

```
clush -w @kairos_all 'apt-get update'
clush -w @kairos_all 'apt-get install apt-transport-https ca-certificates curl software-properties-common -y'
clush -w @kairos_all 'curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -'
clush -w @kairos_all 'apt-key fingerprint 0EBFCD88'
clush -w @kairos_all 'add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"'
clush -w @kairos_all 'apt-get update'
clush -w @kairos_all 'apt-get install docker-ce -y'
clush -w @kairos_all 'apt-cache madison docker-ce'
clush -w @kairos_all 'apt-get install docker-ce=17.06.2~ce-0~ubuntu -y --allow-downgrades'
clush -w @kairos_all 'docker run hello-world'
clush -w @kairos_all 'docker --version'
clush -w @kairos_all 'docker pull sequenceiq/hadoop-docker:2.4.1'
```

---------------------------------------------------------------------------

## Update JAR in docker image

Install mlocate and replace with the 

````
sudo apt-get install mlocate
updatedb
clush -w @kairos_all 'locate hadoop-mapreduce-client-core-2.4.0.jar | xargs ls -l'  > locate-output
cat locate-output | awk '{print substr($1,0,11)" "$10}' > locate-output2
./hack-docker-image-on-all-nodes.sh
```
---------------------------------------------------------------------------

## Running Hadoop

******** only first time when formatting ******

```
clush -w @kairos_master /disk3/bigc/hadoop-2.7.1/bin/hdfs namenode -format
```

After setting up the cluster do

```
JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export JAVA_HOME
mvn package -Pdist -DskipTests -Dtar
./configure-kairos.sh 50 4 hadoop-2.7.1/etc/hadoop/
./copy-config-files.sh 
```

Try to test first if all works fine
```
./start-all.sh
```

```
./run-workload.sh kairos_heavy_tailed_workload generated_workload_kairos 
```

---------------------------------------------------------------------------

## Setting up

```
hadoop-2.7.1/bin/hadoop dfs  -Ddfs.block.size=268435456 -copyFromLocal /disk2/newsmalleraa /random100char-256mb-tiny
hadoop-2.7.1/bin/hadoop dfs  -Ddfs.block.size=268435456 -copyFromLocal /disk2/segment-smallaa /random100char-256mb-small
hadoop-2.7.1/bin/hadoop dfs  -Ddfs.block.size=268435456 -copyFromLocal /disk2/segment-mediumaa /random100char-256mb-medium
hadoop-2.7.1/bin/hadoop dfs  -Ddfs.block.size=268435456 -copyFromLocal /disk2/random-100char-strings-mediumlongaa /random100char-256mb-mediumlong
hadoop-2.7.1/bin/hadoop dfs  -Ddfs.block.size=1000000000 -copyFromLocal /disk2/random-100char-strings-3 /random100char-1gb-long
```


For running our generated workloads you should have a similar output to the following in your hdfs database

```
$ hadoop-2.7.1/bin/hdfs dfs -ls /
18/11/23 16:55:59 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 7 items
-rw-r--r--   3 root supergroup 60000000000 2018-05-10 11:06 /random100char-1gb-long
-rw-r--r--   3 root supergroup  8000000000 2018-05-10 11:16 /random100char-256mb-medium
-rw-r--r--   3 root supergroup 30000000000 2018-05-10 11:14 /random100char-256mb-mediumlong
-rw-r--r--   3 root supergroup  4000000000 2018-05-10 11:22 /random100char-256mb-small
-rw-r--r--   3 root supergroup  2000000000 2018-08-22 21:51 /random100char-256mb-tiny
```

Alternative to run all daemons
```
clush -w @kairos_master /disk3/bigc/hadoop-2.7.1/sbin/hadoop-daemon.sh --script hdfs start namenode
clush -w @kairos_all '/disk3/bigc/hadoop-2.7.1/sbin/hadoop-daemons.sh --script hdfs start datanode'
clush -w @kairos_master '/disk3/bigc/hadoop-2.7.1/sbin/yarn-daemon.sh start resourcemanager'
clush -w @kairos_all '/disk3/bigc/hadoop-2.7.1/sbin/yarn-daemons.sh start nodemanager'
```




