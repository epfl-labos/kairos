# Building and running instructions

----------------------------------------------------------------------------------

## Requirements:

* JDK 1.8
* Maven 3.0 or later
* ProtocolBuffer 2.5.0

----------------------------------------------------------------------------------

## Pre-requisites

Install jdk 1.8, before executing any command dont forget to export JAVA_HOME

Install protobuf 2.5.0

```
$ wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
$ tar xzvf protobuf-2.5.0.tar.gz
$ cd  protobuf-2.5.0
$ ./configure
$ make
$ make check
$ sudo make install
$ sudo ldconfig
$ protoc --version
```

## Compiling code

```
$ git clone git@bitbucket.org:pamedelgado/preemption-yarn-implementation.git
$ cd preemption-yarn-implementation

$ mvn package -Psrc -DskipTests
```

----------------------------------------------------------------------------------

## Building distributions:

Create binary distribution without native code and without documentation:

 ` $ mvn package -Pdist -DskipTests -Dtar `

Create binary distribution with native code and with documentation:

 ` $ mvn package -Pdist,native,docs -DskipTests -Dtar `

Create source distribution:

 ` $ mvn package -Psrc -DskipTests `

Create source and binary distributions with native code and documentation:

 ` $ mvn package -Pdist,native,docs,src -DskipTests -Dtar `

Create a local staging version of the website (in /tmp/hadoop-site)

 ` $ mvn clean site; mvn site:stage -DstagingDirectory=/tmp/hadoop-site `

----------------------------------------------------------------------------------


## Eclipse setup

PREREQUISITES : Do COMPILING section first

```
$ cd hadoop-maven-plugins/
$ mvn install
$ cd ..
$ mvn eclipse:eclipse -DskipTests
```

Download latest version of Eclipse installer, install for java developers

Install eclipse oxygen, point to right vm in eclipse-inst.ini: 
-vm
<pathtojdk>/jdk1.8.0_151/bin/java

before -vmargs

open eclipse, Import existing projects into workspace: 

this are the projects I needed for eclipse not to complain

```
hadoop-annotations, 
hadoop-auth,
hadoop-common,
hadoop-hdfs,
hadoop-kms,
hadoop-minikdc,
hadoop-yarn-api,
hadoop-yarn-common,
hadoop-yarn-server-applicationhistoryservice
hadoop-yarn-server-common
hadoop-yarn-server-nodemanager
hadoop-yarn-server-resourcemanager
hadoop-yarn-server-web-proxy
```

Importing projects to eclipse

When you import the project to eclipse, install hadoop-maven-plugins at first.

```
$ cd hadoop-maven-plugins
$ mvn install
```

Then, generate eclipse project files.

```
$ mvn eclipse:eclipse -DskipTests
```

Import to eclipse by specifying the root directory of the project via
[File] > [Import] > [Existing Projects into Workspace].

----------------------------------------------------------------------------------
Maven build goals:

 * Clean                     : mvn clean
 * Compile                   : mvn compile [-Pnative]
 * Run tests                 : mvn test [-Pnative]
 * Create JAR                : mvn package
 * Run findbugs              : mvn compile findbugs:findbugs
 * Run checkstyle            : mvn compile checkstyle:checkstyle
 * Install JAR in M2 cache   : mvn install
 * Deploy JAR to Maven repo  : mvn deploy
 * Run clover                : mvn test -Pclover [-DcloverLicenseLocation=${user.name}/.clover.license]
 * Run Rat                   : mvn apache-rat:check
 * Build javadocs            : mvn javadoc:javadoc
 * Build distribution        : mvn package [-Pdist][-Pdocs][-Psrc][-Pnative][-Dtar]
 * Change Hadoop version     : mvn versions:set -DnewVersion=NEWVERSION

 Build options:

  * Use -Pnative to compile/bundle native code
  * Use -Pdocs to generate & bundle the documentation in the distribution (using -Pdist)
  * Use -Psrc to create a project source TAR.GZ
  * Use -Dtar to create a TAR with the distribution (using -Pdist)

 Tests options:

  * Use -DskipTests to skip tests when running the following Maven goals:
    'package',  'install', 'deploy' or 'verify'
  * -Dtest=<TESTCLASSNAME>,<TESTCLASSNAME#METHODNAME>,....
  * -Dtest.exclude=<TESTCLASSNAME>
  * -Dtest.exclude.pattern=**/<TESTCLASSNAME1>.java,**/<TESTCLASSNAME2>.java

----------------------------------------------------------------------------------
## Deploying Hadoop


  * Single Node Setup:
    hadoop-project-dist/hadoop-common/SingleCluster.html

  * Cluster Setup:
    hadoop-project-dist/hadoop-common/ClusterSetup.html

----------------------------------------------------------------------------------

