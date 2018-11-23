# KAIROS setting up
===========================================================================

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

```
clush -w @kairos_master /disk3/bigc/hadoop-2.7.1/sbin/hadoop-daemon.sh --script hdfs start namenode
clush -w @kairos_all '/disk3/bigc/hadoop-2.7.1/sbin/hadoop-daemons.sh --script hdfs start datanode'
clush -w @kairos_master '/disk3/bigc/hadoop-2.7.1/sbin/yarn-daemon.sh start resourcemanager'
clush -w @kairos_all '/disk3/bigc/hadoop-2.7.1/sbin/yarn-daemons.sh start nodemanager'
```

---------------------------------------------------------------------------

## Running Hadoop

