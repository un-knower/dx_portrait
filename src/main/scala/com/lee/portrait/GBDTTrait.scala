package com.lee.portrait

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.lee.utils.{FileReporter, PathUtil}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD

import scala.beans.BeanProperty

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/12
  * Time: 14:13
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class GBDTTrait extends PortraitTrait {
  @BeanProperty
  var numIterations: Int = 4
  @BeanProperty
  var numClasses: Int = _
  @BeanProperty
  var maxDepth: Int = 8
  @BeanProperty
  var learnGoal: String = "Classification"
  // Empty categoricalFeaturesInfo indicates all features are continuous.

  //最大叶子节点数
  @BeanProperty
  var maxBins:Integer = _
  var boostingStrategy:BoostingStrategy = _
  /**
    * 初始化
    *
    * @param rdd 模型输入labelpoint rdd
    */
  override def init(rdd: RDD[LabeledPoint], propName: String): Unit = {
    super.init(rdd, propName)
    //模型参数
    if (prop.get("GBDT.learnGoal") != null) {
      learnGoal = prop.getProperty("GBDT.learnGoal")
    }
    boostingStrategy = BoostingStrategy.defaultParams(learnGoal)
    //迭代次数
    if (prop.getProperty("GBDT.numIterations") != null) {
      numIterations = prop.getProperty("GBDT.numIterations").toInt
    }
    boostingStrategy.setNumIterations(numIterations)
    //几分类
    if (prop.get("GBDT.numClasses") != null) {
      numClasses = prop.getProperty("GBDT.numClasses").toInt
      boostingStrategy.getTreeStrategy().setNumClasses(numClasses)
    }
    //树的最大深度
    if (prop.get("GBDT.maxDepth") !=null) {
      maxDepth = prop.getProperty("GBDT.maxDepth").toInt
    }
    boostingStrategy.getTreeStrategy().setMaxDepth(maxDepth)
    /*    //map为空所有特征
    boostingStrategy.treeStrategy.setCategoricalFeaturesInfo(Map[Int,Int]())*/
    //默认  如果配置文件中有就设置
    if (prop.get("GBDT.maxBins") != null) {
      maxBins = prop.getProperty("GBDT.maxBins").toInt
      boostingStrategy.getTreeStrategy().setMaxBins(maxBins)
    }

  }

  override def toString: String = JSON.toJSONString(this,new Array[SerializeFilter](0))

  /**
    * run 方法
    *
    * @param sc
    */
  override def run(sc: SparkContext): RDD[(Double, LabeledPoint)] = {
    val model: GradientBoostedTreesModel = GradientBoostedTrees.train(rddTrain, boostingStrategy)
    var labelAndPreds:RDD[(Double, LabeledPoint)] =null
    if (learnGoal.equals("Classification")) {
      labelAndPreds = rddpre.map { point =>
        val prediction = model.predict(point.features)
        (prediction, point)
      }
    }else{
      labelAndPreds = rddpre.map { point =>
        val prediction = model.predict(point.features)
        (prediction.round.toDouble, point)
      }
    }

    FileReporter.singlton.reportModelStcInfo("Learned classification GBT model:\n" + model.toDebugString)
    model.save(sc, PathUtil.getModelSavePath)
    labelAndPreds
  }
}
