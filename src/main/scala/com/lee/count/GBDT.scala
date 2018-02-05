package com.lee.count

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/30
  * Time: 16:16
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object GBDT {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("testGBDT")
    val sc =new SparkContext(conf)
    val model =  GradientBoostedTreesModel.load(sc,"hdfs://master:8020/user/lihw/output/model/age/GBDT/201801301643")
    val rdd: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc,"hdfs://master:8020/user/lihw/data/agesvmdata/20180130")
    val count = rdd.filter(lp => {
      val predict = model.predict(lp.features)
      val cha = predict.round.toInt - lp.label
      cha <= 5
    }).count()
    val res = 1.0 * count / rdd.count()
    println(res)

  }
}
