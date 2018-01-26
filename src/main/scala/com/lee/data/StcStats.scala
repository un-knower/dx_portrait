package com.lee.data

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.annotation.JSONField
import com.alibaba.fastjson.serializer.SerializeFilter

import scala.beans.BeanProperty

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/12
  * Time: 15:08
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

class StcStats {
  @JSONField
  @BeanProperty
  var accuracy :Double = _
  @JSONField
  @BeanProperty
  var precision:Double = _
  @JSONField
  @BeanProperty
  var recall :Double = _
  @JSONField
  @BeanProperty
  var roc:Double = _
  @JSONField
  @BeanProperty
  var err:Double = _

  @JSONField
  @BeanProperty
  var fn:Long = _

  @JSONField
  @BeanProperty
  var fp:Long = _

  @JSONField
  @BeanProperty
  var tp:Long = _

  @JSONField
  @BeanProperty
  var tn:Long = _


  override def toString: String = JSON.toJSONString(this,new Array[SerializeFilter](0))
}
