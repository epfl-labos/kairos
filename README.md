# KAIROS

What is it?
-----------

Kairos
------

Kairos is a preemptive data center scheduler that does not rely on task runtime estimates. It was presented at the 9th ACM Symposium on Cloud Computing [SoCC 2018](https://infoscience.epfl.ch/record/256720/files/socc18-final186.pdf).

Kairos:
1) Shows good data center scheduling performance without using task runtime estimates.
2) Introduces an efficient distributed version of the LAS scheduling discipline.
3) Implements this distributed approximation of LAS in YARN, and compare its performance to state-of-the-art alternatives by measurement and simulation.

Install and compile
-------------------

For compile, please refer to BUILDING.md for detail. Kairos is built on Hadoop-2.7.1 and it depends on ProtocolBuffer 2.5.0 (higher version may report error).


Docker image
------------
 
Please use /sequenceiq/hadoop-docker as the docker image for running applications. 
We have tested /sequenceiq/hadoop-docker:2.4.0, and it can
support both Hadoop Mapreduce and Spark. For configuring YARN with docker support, please refer this:

https://hadoop.apache.org/docs/r2.7.2/hadoop-yarn/hadoop-yarn-site/DockerContainerExecutor.html

We have hacked NodeManager. Following configurations below will have all your applications (both MapReduce and Spark) running
in Docker containers: 

In yarn-site.xml

```
 <property><name>yarn.nodemanager.docker-container-executor.image-name</name><value>sequenceiq/hadoop-docker:2.4.0</value> </property>
 <property><name>yarn.nodemanager.container-executor.class</name><value>org.apache.hadoop.yarn.server.nodemanager.DockerContainerExecutor</value></property>
 <property><name>yarn.nodemanager.docker-container-executor.exec-name</name><value>/usr/bin/docker(path to your docker)</value></property>
````

Configuration for preemption
----------------------------

In yarn-site.xml

```
<property><name>yarn.resourcemanager.scheduler.monitor.enable</name><value>True</value></property>
```
Enable resource monitor for Capacity Scheduler.

```
<property><name>yarn.resourcemanager.monitor.capacity.preemption.suspend</name><value>True</value></property>
```

Enable suspension based preemption for Capacity Scheduler. If this option is False, then only killing based preemption will be
applied. 

In capacity-site.xml

```
<property><name>yarn.scheduler.capacity.root.default.maxresumptopportunity</name><value>3</value></property>
<property><name>yarn.scheduler.capacity.root.default.pr_number</name><value>2</value></property>
```

Contact
-------

- Pamela Delgado <pamela.delgado@epfl.ch>
- Florin Dinu <florin.dinu@epfl.ch>

