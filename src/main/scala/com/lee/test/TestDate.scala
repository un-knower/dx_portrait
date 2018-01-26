package com.lee.test

import com.lee.utils.DateUtil

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/26
  * Time: 9:41
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object TestDate {
  def main(args: Array[String]): Unit = {
    val pattern = DateUtil.getCurDataTimeByPattern("yyyy-MM-dd")
    println(pattern)
    println("2017-01-09" > "2017-02-09")
    println("2017-03-09" > "2017-02-09")
    println("2018-01-09" > "2017-01-09")
    println("2017-08-09" > "2017-02-09")
    println("2017-07-09" > "2017-02-09")
    println("2017-01-09" > "2017-01-10")
  }
}
