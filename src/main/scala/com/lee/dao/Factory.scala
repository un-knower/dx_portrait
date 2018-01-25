package com.lee.dao

import com.lee.service._

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/11
  * Time: 11:01
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object Factory {

  def getService(model:String):ServiceTrait ={
    model match {
      case "LBFGS" => new LBFGSService()
      case "DecisionTree" => new DecisionTreeService()
      case "GBDT" => new GBDTService()
      case "RandomForest" => new RandomForestService()
      case _ => null
    }
  }
}
