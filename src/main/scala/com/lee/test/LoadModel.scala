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
    val model = DecisionTreeModel.load(sc,"hdfs://master:8020/user/lihw/output/model/sex/DecisionTree/201801251750")
    println(model.toDebugString)
  }
}
