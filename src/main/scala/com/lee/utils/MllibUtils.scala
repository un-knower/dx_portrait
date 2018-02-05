package com.lee.utils

import com.lee.data.StcStats
import org.apache.log4j.Logger
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD

/**
  * Created by root on 2017/8/1.
  */
object MllibUtils {
  private val log = Logger.getLogger(getClass)

  /**
    * 正样本预测为正样本的数量
    *
    * @param predictRdd
    * @return
    */
  def getTp(predictRdd: RDD[(Int, Int)]): Long = {
    predictRdd.filter(line => line._1 == 1 && line._2 == 1).count()
  }

  /**
    * 负样本预测负样本的数量
    *
    * @param predictRdd
    * @return
    */
  def getTn(predictRdd: RDD[(Int, Int)]): Long = {
    predictRdd.filter(line => line._1 == 0 && line._2 == 0).count()
  }

  /**
    * 负样本预测为正样本的数量
    *
    * @param predictRdd
    * @return
    */
  def getFp(predictRdd: RDD[(Int, Int)]): Long = {
    predictRdd.filter(line => line._1 == 1 && line._2 == 0).count()
  }

  /**
    * 正样本预测为翻样本
    *
    * @param predictRdd
    * @return
    */
  def getFn(predictRdd: RDD[(Int, Int)]): Long = {
    predictRdd.filter(line => line._1 == 0 && line._2 == 1).count()
  }

  /**
    * 得到准确率
    *
    * @param predictRdd
    * @return
    */
  def getAccuracy(predictRdd: RDD[(Int, Int)]): Double = {
    1.0 * predictRdd.filter(line => line._1 == line._2).count() / predictRdd.count()
  }


  /**
    * 得到精确率
    *
    * @param predictRdd
    * @return
    */
  def getPrecision(predictRdd: RDD[(Int, Int)]): Double = {
    val tp = getTp(predictRdd)
    val fp = getFp(predictRdd)
    1.0 * tp / (tp + fp)
  }

  /**
    * 得到召回率
    *
    * @param predictRdd
    * @return
    */
  def getRecall(predictRdd: RDD[(Int, Int)]): Double = {
    val tp = getTp(predictRdd)
    val fn = getFn(predictRdd)
    1.0 * tp / (tp + fn)
  }

  def getRoc(predictRdd: RDD[(Int, Int)]): Double = {
    val rdd = predictRdd.map(line => (line._1.toDouble, line._2.toDouble))
    val metrics = new BinaryClassificationMetrics(rdd)
    val d: Double = metrics.areaUnderROC()
    d
  }

  def getErr(predictRdd: RDD[(Int, Int)]): Double = {
    1.0 * predictRdd.filter(line => line._1 != line._2).count() / predictRdd.count()
  }

  /**
    * 第一个是与测试  第二个是真实值
    *
    * @param predictRdd
    * @return
    */
  def print(predictRdd: RDD[(Int, Int)]): StcStats = {
    predictRdd.cache()
    val collect = predictRdd.map(line => {
      line._2
    }).distinct().collect()

    val stats = new StcStats()
    if (collect.size == 2) {
      val tp = getTp(predictRdd)
      val tn = getTn(predictRdd)
      val fn = getFn(predictRdd)
      val fp = getFp(predictRdd)
      stats.tp = tp
      stats.tn = tn
      stats.fn = fn
      stats.fp = fp
      stats.accuracy = getAccuracy(predictRdd)
      stats.precision = getPrecision(predictRdd)
      stats.recall = getRecall(predictRdd)
      stats.roc = getRoc(predictRdd)
      stats.err = getErr(predictRdd)
    }else{
      val tp = getTp(predictRdd)
      val tn = getTn(predictRdd)
      val fn = getFn(predictRdd)
      val fp = getFp(predictRdd)
      stats.tp = tp
      stats.tn = tn
      stats.fn = fn
      stats.fp = fp
      stats.accuracy = getAccuracy(predictRdd)
      stats.precision = getPrecision(predictRdd)
      stats.recall = getRecall(predictRdd)
      stats.roc = getRoc(predictRdd)
      stats.err = getErr(predictRdd)
    }
    stats
  }
}
