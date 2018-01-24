package com.lee.service

import com.lee.main.JobArgs
import com.lee.portrait.GBDTTrait
import com.lee.utils.FileReporter
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/12
  * Time: 15:17
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class GBDTService extends ServiceTrait {

  private var gbdtTrait: GBDTTrait = _

  /**
    * 初始化
    *
    * @param reporter
    */
  override def init(sparkContext: SparkContext): Unit = {
    super.init(sparkContext)
    gbdtTrait = new GBDTTrait()
  }

  /**
    * 模型训练
    *
    * @param modelInputRdd
    * @return
    */
  override def traitRun(modelInputRdd: RDD[LabeledPoint]): RDD[(Double, LabeledPoint)] = {
    gbdtTrait.init(modelInputRdd, JobArgs.model + ".properties")
    FileReporter.singlton.reportModelStcInfo(gbdtTrait.toString)
    gbdtTrait.run(sc)
  }
}
