package com.lee.service

import com.lee.main.JobArgs
import com.lee.portrait.PortraitTrait
import com.lee.utils._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

/**
  * Created by root on 2018/1/11.
  */
trait ServiceTrait extends LogUtils {
  var sc: SparkContext = _
  var traits: PortraitTrait = _

  /**
    * 初始化
    * @param sparkContext
    */
  def init(sparkContext: SparkContext) = {
    log.info("service init")
    sc = sparkContext
  }

  /**
    * 训练数据转labelpoint
    *
    * @param orgin_trait_data
    * @return
    */
  def getLabelPointRdd(orgin_trait_data: RDD[String]) = ???

  /**
    * stc模型评估
    *
    * @param predict_test_rdd
    */
  def stc(predict_test_rdd: RDD[(Double, LabeledPoint)]): Unit = {
    val stcRdd = predict_test_rdd.map(line => {
      (line._1.toInt, line._2.label.toInt)
    })
    FileReporter.singlton.reportModelStcInfo(MllibUtils.print(stcRdd).toString)
  }

  /**
    * 预测电信数据
    *
    * @return
    */
  def predictDxDate() = ???

  /**
    * 更新电信心别
    *
    * @return
    */
  def updateDxSex() = ???

  def traitDate2Svm() = {
    //TODO 处理训练数据喂svm格式
    /*if (!HDFSUtil.exists(PathUtil.ModelinputSvmPath)) {
      log.error("no svm data,model exit")
    }*/
  }

  /**
    * run方法
    *
    * @return
    */
  def run() = {
    log.info("in service run method")
    //读取输入出去成rdd
    log.info("read traitDataPath to rdd")
    traitDate2Svm()
    //处理成labelpointrdd
    log.info("deal rdd 2 labelpoint")
    val modelInputRdd: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc,PathUtil.getSvmSavePath)
    //模型训练
    val predict_test_rdd: RDD[(Double, LabeledPoint)] = traitRun(modelInputRdd)
    //stc统计
    log.info("model stc")
    stc(predict_test_rdd)
    //模型预测
    log.info("start predict dxdata")
    //TODO 预测电信数据
    //predictDxDate()
    //更新性别
    log.info("update dx sex")
    //TODO 更新电信的性别
    //updateDxSex()
  }

  /**
    * 模型训练
    *
    * @param modelInputRdd
    * @return
    */
  def traitRun(modelInputRdd: RDD[LabeledPoint]): RDD[(Double, LabeledPoint)] = ???
}



