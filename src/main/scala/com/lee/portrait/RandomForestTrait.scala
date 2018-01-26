package com.lee.portrait

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.lee.utils.PathUtil
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

import scala.beans.BeanProperty

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/15
  * Time: 10:23
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class RandomForestTrait extends PortraitTrait {
  @BeanProperty
  var numClasses: Int = 2
  @BeanProperty
  var numTree: Int = 2
  @BeanProperty
  var maxDepth: Int = 5
  @BeanProperty
  var maxBins: Int = 3
  @BeanProperty
  var impurity: String = "gini"
  @BeanProperty
  var seed: Int = 12345
  @BeanProperty
  var featureSubsetStrategy: String = "auto"
  var categoricalFeaturesInfo = Map[Int, Int]()


  /**
    * 初始化
    *
    * @param rdd 模型输入labelpoint rdd
    */
  override def init(rdd: RDD[LabeledPoint], propName: String): Unit = {
    super.init(rdd, propName)
    if (prop.get("RandomForest.numClasses") != null) {
      numClasses = prop.getProperty("RandomForest.numClasses").toInt
    }
    if (prop.get("RandomForest.numTree") != null) {
      numClasses = prop.getProperty("RandomForest.numTree").toInt
    }
    if (prop.get("RandomForest.maxDepth") != null) {
      maxDepth = prop.getProperty("RandomForest.maxDepth").toInt
    }
    if (prop.get("RandomForest.maxBins") != null) {
      maxBins = prop.getProperty("RandomForest.maxBins").toInt
    }
    if (prop.get("RandomForest.impurity") != null) {
      impurity = prop.getProperty("RandomForest.impurity")
    }
    if (prop.get("RandomForest.featureSubsetStrategy") != null) {
      featureSubsetStrategy = prop.getProperty("RandomForest.featureSubsetStrategy")
    }
    if (prop.get("RandomForest.seed") != null) {
      seed = prop.getProperty("RandomForest.seed").toInt
    }
  }

  /**
    * run 方法
    *
    * @param sc
    */
  override def run(sc: SparkContext): RDD[(Double, LabeledPoint)] = {
    RandomForestTrait.model = RandomForest.trainClassifier(rddTrain, numClasses, categoricalFeaturesInfo, numTree, featureSubsetStrategy, impurity, maxDepth, maxBins, seed)
    val labelAndPreds = rddpre.map { point =>
      val prediction = RandomForestTrait.model.predict(point.features)
      (prediction, point)
    }
    RandomForestTrait.model.save(sc, PathUtil.getModelSavePath)
    labelAndPreds
  }
  override def toString: String = JSON.toJSONString(this,new Array[SerializeFilter](0))
}

object RandomForestTrait {
  var model: RandomForestModel = _
}