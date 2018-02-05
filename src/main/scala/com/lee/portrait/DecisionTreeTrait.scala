package com.lee.portrait

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.lee.utils.{FileReporter, PathUtil, PropUtil}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import scala.beans.BeanProperty

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/11
  * Time: 13:50
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class DecisionTreeTrait extends PortraitTrait {
  @BeanProperty
  var numClasses: Int = 2
  @BeanProperty
  var maxDepth: Int = 5
  @BeanProperty
  var maxBins: Int = 3
  @BeanProperty
  var impurity: String = "gini"
  var categoricalFeaturesInfo = Map[Int, Int]()

  /**
    * 初始化
    *
    * @param modelPath 模型保存路径
    * @param rdd       模型输入labelpoint rdd
    */
  override def init(rdd: RDD[LabeledPoint], propName: String): Unit = {
    super.init(rdd: RDD[LabeledPoint], propName: String)
    //加载模型参数
    if (prop.getProperty("DecisionTree.numClasses") != null) {
      numClasses = prop.getProperty("DecisionTree.numClasses").toInt
    }
    if (prop.getProperty("DecisionTree.maxDepth") != null) {
      maxDepth = prop.getProperty("DecisionTree.maxDepth").toInt
    }
    if (prop.getProperty("DecisionTree.maxBins") != null) {
      maxBins = prop.getProperty("DecisionTree.maxBins").toInt
    }
    if (prop.getProperty("DecisionTree.impurity") != null) {
      impurity = prop.getProperty("DecisionTree.impurity")
    }
  }

  /**
    * run 方法
    *
    * @param sc
    */
  override def run(sc: SparkContext) = {
    val model: DecisionTreeModel = DecisionTree.trainClassifier(rddTrain, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    model.save(sc, PathUtil.getModelSavePath)
    FileReporter.singlton.reportModelStcInfo("Learned classification DecisionTree model:\n"+model.toDebugString)
    rddpre.map { point =>
      val prediction: Double = model.predict(point.features)
      (prediction, point)
    }
  }

  override def toString: String = JSON.toJSONString(this, new Array[SerializeFilter](0))
}
