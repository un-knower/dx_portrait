package com.lee.count

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/29
  * Time: 11:19
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object PredictErr {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SampleCount")
    val sc = new SparkContext(conf)
    val sample = sc.textFile("hdfs://master:8020//user/lihw/output/samplecount")
    sample.filter(line=>{
      val split = line.split(",")
      !split(0).split("：")(1).equals(split(1).split("：")(1))
    }).saveAsTextFile("hdfs://master:8020//user/lihw/output/samplecounterr")
  }
}
