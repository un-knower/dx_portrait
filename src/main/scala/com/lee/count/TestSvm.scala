package com.lee.count

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/30
  * Time: 11:06
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object TestSvm {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SampleCount")
    val sc = new SparkContext(conf)
    val sample = sc.textFile("hdfs://master:8020/user/lihw/data/sexsvmdata/20180130")
    sample.map(line=>{
      line.split(" ")(1)
      line
    }).take(10).foreach(println)
  }
}
