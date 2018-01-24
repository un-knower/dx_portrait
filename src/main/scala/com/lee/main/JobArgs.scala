package com.lee.main

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter

import scala.beans.BeanProperty

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/11
  * Time: 9:53
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object JobArgs {
  @BeanProperty
  var basePath: String = _
  @BeanProperty
  var outputPath: String = _
  @BeanProperty
  var model: String = _
  @BeanProperty
  var sparkLogLevel:String = _
  @BeanProperty
  var fsDefault:String = _
  @BeanProperty
  var sampleCount:String = _

  override def toString: String = JSON.toJSONString(this,new Array[SerializeFilter](0))

}
