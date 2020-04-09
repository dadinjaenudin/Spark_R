---
title: 'Spark R'
disqus: Dadin Jaenudin
---

# Running Spark in R

## Table of Contents

[TOC]

I have 2 mechine will run spark
 
1. IBM x3850 with OS Redhat 
```gherkin=
## Install Java
java -version
openjdk version "1.8.0_102"
OpenJDK Runtime Environment (build 1.8.0_102-b14)
OpenJDK 64-Bit Server VM (build 25.102-b14, mixed mode)
```

2. IBM Power Linux with os Ubuntu
We can download java for Power Linux Here


```gherkin=

$ wget http://public.dhe.ibm.com/ibmdl/export/pub/systems/cloud/runtimes/java/8.0.6.7/linux/ppc64le/ibm-java-ppc64le-sdk-8.0-6.7.bin

$ bin ibm-java-ppc64le-sdk-8.0-6.7.bin
```


## Install Spark

Download spark 
https://spark.apache.org/downloads.html

1. IBM x3850 with OS Redhat 
```gherkin=
$ cd /opt
$ tar xvf spark-2.4.3-bin-hadoop2.7.tgz
$ vi /opt/spark-2.4.3-bin-hadoop2.7/conf/spark-envs.sh
export SPARK_MASTER_HOST=hdpmaster1.xxxx.com
export SPARK_LOCAL_IP=172.16.67.206
export SPARK_MASTER_PORT=7078
export SPARK_MASTER_WEBUI_PORT=8180
#export SPARK_WORKER_MEMORY=5G
#export SPARK_WORKER_INSTANCES=2

$ vi /opt/spark-2.4.3-bin-hadoop2.7/conf/slaves
hdpmaster.xxx.com
node-master.xxx.com

$ cd /opt/spark-2.4.3-bin-hadoop2.7
$ vi run_spark_master.sh
export SPARK_HOME=/opt/spark-2.4.3-bin-hadoop2.7/
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./stop-master.sh
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./start-master.sh

# start slave
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./stop-slave.sh
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./start-slave.sh spark://hdpmaster1.xxx.com:7078

$ vi run_stop_spark.sh
export SPARK_HOME=/opt/spark-2.4.3-bin-hadoop2.7/
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./stop-master.sh

# start slave
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./stop-slave.sh

$ chmod +x run_spark_master.sh
$ chmod +x run_stop_spark.sh

# run spark master 
./run_spark_master.sh


```

1. IBM Power Linux OS Ubuntu
```gherkin=
$ cd /opt
$ tar xvf spark-2.4.3-bin-hadoop2.7.tgz

$ vi /opt/spark-2.4.3-bin-hadoop2.7/conf/slaves
hdpmaster.xxx.com
node-master.xxx.com

$ cd /opt/spark-2.4.3-bin-hadoop2.7
$ vi run_spark_slave.sh
# start slave
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./stop-slave.sh
/opt/spark-2.4.3-bin-hadoop2.7/sbin/./start-slave.sh spark://hdpmaster1.xxx.com:7078

$ chmod +x run_spark_slave.sh

# run spark slave
./run_spark_slave.sh


```


Spark R Script
---

```gherkin=
# spark.R

# Set memory allocation for whole local Spark instance
Sys.setenv("SPARK_MEM" = "50g")
config <- spark_config()
#User Memory
config[["sparklyr.shell.driver-memory"]] <- "100G"
config[["sparklyr.shell.executor-memory"]] <- "100G"
config[["maximizeResourceAllocation"]] <- "TRUE"
config[["spark.default.parallelism"]] <- 32
config[["spark.sql.catalogImplementation"]] <- "in-memory"
config[["spark.driver.memoryOverhead"]] = "100g" 
config[["sparklyr.gateway.start.timeout"]] <- 10

config[["sparklyr.shell.conf"]] <- "spark.driver.extraJavaOptions=-XX:MaxHeapSize=50G"
config[["spark.driver.maxResultSize"]] <- "30G"
config[["spark.dynamicAllocation.enable"]] <- "TRUE"
config[["spark.sql.shuffle.partitions"]] <- 400 

config[["spark.driver.extraJavaOptions"]] <- "-Xmx6G -XX:MaxPermSize=20G -XX:+UseCompressedOops -XX:UseGCOverheadLimit=50G" 

#spark_disconnect(sc)
sc <- spark_connect(master = "spark://172.16.67.206:7078",
                    version = "2.4.3",
                    config = config,
                    spark_home = "/opt/spark-2.4.3-bin-hadoop2.7")


# Read from CSV 
cur_year_pattern <- paste0(data_sales,"sales_article_",cur_year,"**",".csv")
current_year_sales_sc <- spark_read_csv(sc, "current_year_sales_sc",
                                        path = cur_year_pattern, 
                                        header=T, 
                                        overwrite=T,
                                        memory=F,
                                        infer_schema = FALSE
                                        )
                                        
```

Run script 

```gherkin=
$ Rscript --no-save --no-restore --verbose spark.R 
```

We can see spark UI in 
http://localhost:8180/

![](https://i.imgur.com/sGFHJRu.png)
