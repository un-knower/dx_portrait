package com.lee.utils

import com.lee.conf.Contants
import com.lee.dao.FileDao

import scala.collection.mutable

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/22
  * Time: 10:16
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object FIles {
  var app_name :mutable.HashMap[String,String] = _
  var app_class_name :mutable.HashMap[String,mutable.HashMap[String,String]] = _
  def init(): Unit ={
    app_class_name = FileDao.getAppClassName(PathUtil.configPathByName(Contants.APP_CLASS_PATH))
    app_name = FileDao.getAppFile2Map(PathUtil.configPathByName(Contants.APP_PATH))
  }
}
