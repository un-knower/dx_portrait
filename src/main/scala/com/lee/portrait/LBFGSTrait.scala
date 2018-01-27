package com.lee.portrait

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.lee.utils.{FileReporter, HDFSUtil, PathUtil, PropUtil}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.{LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import scala.beans.BeanProperty

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/10
  * Time: 17:15
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class LBFGSTrait extends PortraitTrait {
  //模型参数
  @BeanProperty
  var numCorrections: Int = 10
  @BeanProperty
  var maxNumIterations: Int = 20
  @BeanProperty
  var regParam: Double = 0.05
  @BeanProperty
  var numFeatures: Int = _
  @BeanProperty
  var convergenceTol: Double = 1e-4

  var initialWeightsWithIntercept: Vector = _

  override def init(rdd: RDD[LabeledPoint],propName:String) = {
    super.init(rdd: RDD[LabeledPoint],propName:String)
    //加载模型参数
    val prop = PropUtil.getPropByName("LBFGS.properties")
    if (prop.getProperty("LBFGS.numCorrections") != null) numCorrections = prop.getProperty("LBFGS.numCorrections").toInt
    if (prop.getProperty("LBFGS.convergenceTol") != null) convergenceTol = prop.getProperty("LBFGS.convergenceTol").toDouble
    if (prop.getProperty("LBFGS.maxNumIterations") != null) maxNumIterations = prop.getProperty("LBFGS.maxNumIterations").toInt
    if (prop.getProperty("LBFGS.regParam") != null) regParam = prop.getProperty("LBFGS.regParam").toDouble
    //特征的数量
    numFeatures = lpRdd.take(1)(0).features.size
  }
  override def run(sc: SparkContext):  RDD[(Double, LabeledPoint)] = {

    //处理成label vector
    val training: RDD[(Double, Vector)] = rddTrain.map(x => (x.label, MLUtils.appendBias(x.features)))
    training.cache()
    //模型参数
    initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))
    //训练模型
    val (weightsWithIntercept, loss) = LBFGS.runLBFGS(
      training,
      new LogisticGradient(),
      new SquaredL2Updater(),
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)
    //得到模型
    val model = new LogisticRegressionModel(
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
      weightsWithIntercept(weightsWithIntercept.size - 1))
    //loss
    FileReporter.singlton.reportModelStcInfo("Loss of each step in training process:\n"+loss.mkString("\n"))
    FileReporter.singlton.reportModelStcInfo("Learned LBFGS model weights:\n" + model.weights)

    //预测
    rddpre.map(line => {
      val prediction: Double = model.predict(line.features)
      (prediction, line)
    })
  }
  override def toString: String = JSON.toJSONString(this,new Array[SerializeFilter](0))

}
