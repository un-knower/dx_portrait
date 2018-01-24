package com.lee.test

import org.apache.commons.lang.StringUtils

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/22
  * Time: 14:13
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object TestHour {
  def main(args: Array[String]): Unit = {
    val h ="000000000001011000000010"
    println(StringUtils.countMatches(h,"1"))
    println(h.substring(0,5))
    println(h.substring(5,9))
    println(h.substring(9,13))
    println(h.substring(13,15))
    println(h.substring(15,19))
    println(h.substring(19,24))
  }
}
