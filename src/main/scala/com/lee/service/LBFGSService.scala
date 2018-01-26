package com.lee.service

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.lee.main.JobArgs
import com.lee.portrait.LBFGSTrait
import com.lee.utils.{FileReporter, MllibUtils}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/11
  * Time: 9:44
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class LBFGSService extends ServiceTrait {

  private var lbfgsTrait: LBFGSTrait = _

  override def init(sc: SparkContext) = {
    super.init(sc)
    lbfgsTrait = new LBFGSTrait()
  }


  override def traitRun(modelInputRdd: RDD[LabeledPoint]): RDD[(Double, LabeledPoint)] = {
    //模型初始化
    log.info("init LBFGS model args")
    lbfgsTrait.init(modelInputRdd,JobArgs.model+".properties")
    //记录模型参数
    FileReporter.singlton.reportModelStcInfo(lbfgsTrait.toString)
    //模型训练
    log.info("start LBFGS tarit model")
    lbfgsTrait.run(sc)
  }

  /**
    * stc模型评估
    *
    * @param predict_test_rdd
    */
  override def stc(predict_test_rdd: RDD[(Double, LabeledPoint)]): Unit = {
    val metrics = new BinaryClassificationMetrics(predict_test_rdd.map(line=>(line._1,line._2.label)))
    val auROC = metrics.areaUnderROC
    log.info("Area under ROC = " + auROC)
    super.stc(predict_test_rdd)
  }

}
