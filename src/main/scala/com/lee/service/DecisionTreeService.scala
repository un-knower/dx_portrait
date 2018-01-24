package com.lee.service

import com.lee.main.JobArgs
import com.lee.portrait.DecisionTreeTrait
import com.lee.utils.FileReporter
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/11
  * Time: 14:00
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class DecisionTreeService extends ServiceTrait {
  private var decesionTree: DecisionTreeTrait = _

  /**
    * 初始化
    *
    * @param reporter
    */
  override def init(sc:SparkContext): Unit = {
    super.init(sc)
    decesionTree = new DecisionTreeTrait
  }

  /**
    * 模型训练
    *
    * @param modelInputRdd
    * @return
    */
  override def traitRun(modelInputRdd: RDD[LabeledPoint]): RDD[(Double, LabeledPoint)] ={
    //模型初始化
    log.info("init DecisionTree model args")
    decesionTree.init( modelInputRdd,JobArgs.model+".properties")
    //记录模型参数
    FileReporter.singlton.reportModelStcInfo(decesionTree.toString)
    //模型训练
    log.info("start DecisionTree tarit model")
    decesionTree.run(sc)
  }
}
