package com.lee.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.model.DecisionTreeModel

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/25
  * Time: 17:56
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object LoadModel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val parallelize = sc.parallelize(Array(1,2,3))
    parallelize.foreach(println)
  }
}
