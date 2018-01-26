package com.lee.main

import com.lee.dao.Factory
import com.lee.service.{DataService, ServiceTrait}
import com.lee.utils.{FIles, FileReporter, HDFSUtil, PropUtil}
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/11
  * Time: 9:51
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object MainCtrl {
  var sc: SparkContext = _
  val log = Logger.getLogger(getClass)

  private var serviceTrait: ServiceTrait = _

  def sparkInit() = {
    val conf = new SparkConf()
    val prop = PropUtil.getPropByName("spark.properties")
    prop.entrySet().foreach(tup => {
      conf.set(tup.getKey.toString, tup.getValue.toString)
    })
    conf.setAppName(getClass.getSimpleName)
    sc = new SparkContext(conf)
    if (JobArgs.sparkLogLevel != null) {
      sc.setLogLevel(JobArgs.sparkLogLevel)
    }
  }

  def init(s: String) = {
    PropUtil.init(s)
    if (PropUtil.getProperty("trait.root.path") != null) {
      JobArgs.basePath = PropUtil.getProperty("trait.root.path")
    }

    if (PropUtil.getProperty("trait.output.path") != null) {
      JobArgs.outputPath = PropUtil.getProperty("trait.output.path")
    }
    if (PropUtil.getProperty("trait.model") != null) {
      JobArgs.model = PropUtil.getProperty("trait.model")
    }
    if (PropUtil.getProperty("spark.log.level") != null) {
      JobArgs.sparkLogLevel = PropUtil.getProperty("spark.log.level")
    }
    if (PropUtil.getProperty("sample.data.count") != null) {
      JobArgs.sampleCount = PropUtil.prop.getProperty("sample.data.count", "0")
    }
    if (PropUtil.getProperty("fs.default") != null) {
      JobArgs.fsDefault = PropUtil.getProperty("fs.default")
      HDFSUtil.init(PropUtil.getProperty("fs.default"))
    }
    if (PropUtil.getProperty("model.name") != null) {
      JobArgs.modelName = PropUtil.getProperty("model.name")
    }
    FileReporter.singlton.init()
    sparkInit()
    //数据服务
    serviceTrait = Factory.getService(JobArgs.model)
    if (serviceTrait == null) {
      println("no service" + JobArgs.model)
    } else {
      serviceTrait.init(sc)
    }
    //训练服务
    //文件
    FIles.init()
  }

  def run() = {
    //数据服务
    DataService.init(sc)
    //run
    DataService.run()
    //模型
    if (serviceTrait != null) {
      serviceTrait.run()
    }
  }

  def check() = {
    if (StringUtils.isEmpty(JobArgs.model)
      || StringUtils.isEmpty(JobArgs.basePath)
      || StringUtils.isEmpty(JobArgs.outputPath)
      || StringUtils.isEmpty(JobArgs.modelName)
      || StringUtils.isEmpty(JobArgs.fsDefault)) {
      println("JobArgs is error")
      sys.exit(1)
    }
    FileReporter.singlton.reportModelStcInfo(JobArgs.toString)
  }

  def toolRun(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("must input config name")
      sys.exit(1)
    }
    init(args(0))
    check()
    run()
  }

  def main(args: Array[String]): Unit = {
    toolRun(args)
  }
}
