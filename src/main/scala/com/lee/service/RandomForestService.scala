package com.lee.service

import com.lee.main.JobArgs
import com.lee.portrait.RandomForestTrait
import com.lee.utils.FileReporter
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/15
  * Time: 10:49
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class RandomForestService extends ServiceTrait{
 var randomForestTrait :RandomForestTrait = _

  /**
    * 初始化
    *
    * @param sparkContext
    */
  override def init(sparkContext: SparkContext): Unit = {
    super.init(sparkContext)
    randomForestTrait = new RandomForestTrait()
  }

  /**
    * 模型训练
    *
    * @param modelInputRdd
    * @return
    */
  override def traitRun(modelInputRdd: RDD[LabeledPoint]): RDD[(Double, LabeledPoint)] = {
    randomForestTrait.init(modelInputRdd,JobArgs.model+".properties")
    FileReporter.singlton.reportModelStcInfo(randomForestTrait.toString)
    randomForestTrait.run(sc)

  }
}
