package com.lee.portrait

import java.util.Properties

import com.lee.utils.{FileReporter, LogUtils, PropUtil}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by root on 2018/1/10.
  */
trait PortraitTrait extends LogUtils {
  var sc: SparkContext = _
  var lpRdd: RDD[LabeledPoint] = _
  var rddTrain: RDD[LabeledPoint] = _
  var rddpre: RDD[LabeledPoint] = _
  var splitArr: Array[Double] = _
  var prop:Properties = _

  /**
    * 初始化
    *
    * @param rdd       模型输入labelpoint rdd
    */
  def init(rdd: RDD[LabeledPoint],propName:String) :Unit= {
    prop = PropUtil.getPropByName(propName)
    lpRdd = rdd
    lpRdd.cache()
    //切分比例
    if (prop.getProperty("split.arr") != null) {
      splitArr = prop.getProperty("split.arr").split(",").map(_.toDouble)
    }
    //切分训练数据
    if (splitArr != null && splitArr.length == 2) {
      val rddSplit: Array[RDD[LabeledPoint]] = rdd.randomSplit(splitArr)
      rddTrain = rddSplit(0)
      rddpre = rddSplit(1)
    } else {
      rddTrain = rdd
      rddpre = rdd
    }
  }

  /**
    * run 方法
    *
    * @param sc
    */
  def run(sc: SparkContext):RDD[(Double, LabeledPoint)] = ???

}
