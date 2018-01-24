#!/bin/sh

sudo -u spark spark-submit \
--class com.lee.main.MainCtrl \
--master yarn \
--driver-class-path /usr/hdp/2.5.0.0-1245/hadoop/lib/hadoop-ks3-0.1.jar \
--deploy-mode cluster \
--queue ups \
--executor-memory 4G \
--num-executors 11 \
--executor-cores 2 \
--driver-memory 4G \
--driver-cores 2 \
--jars $(echo /home/wylog/lhw/libs/*jar | sed 's/ /,/g') \
--conf spark.yarn.executor.memoryOverhead=1048 \
--conf spark.yarn.driver.memoryOverhead=1048 \
/data/dmp/portrait/lihw/jar/dx_portrait-1.0.jar net.properties