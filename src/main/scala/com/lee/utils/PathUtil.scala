package com.lee.utils

import com.lee.main
import com.lee.main.JobArgs

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/12
  * Time: 11:09
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object PathUtil {
  val date = DateUtil.getCurrentDateStr()
  val minStr = DateUtil.getCurMinStr()

  //mongo 输出
  def getMongoSampleDataPath:String = JobArgs.basePath+"/data/trainsample/"+date
  //svm 保存路径
  def getSvmSavePath = JobArgs.basePath+"/data/svmdata/"+date+"/"
  //模型输出路径
  def getModelSavePath:String = JobArgs.outputPath+"/model/"+JobArgs.model+"/"+minStr
  //配置文件
  def configPathByName(name: String) = JobArgs.basePath + "/config/" + name
  //得到日志路径
  def getLogPath = JobArgs.basePath+"/log/"
  //特征索引
  def getFeatureIndex = JobArgs.basePath+ "/config/" + date+"_feature_index.txt"
  //sample 统计文件
  def getSampleCountPath(name:String) = JobArgs.outputPath+"/count/"+name
  //mongo orgin
  def getMongoOrginPath = JobArgs.basePath+"/data/orgindata/"+date+"/"
}
