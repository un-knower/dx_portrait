package com.lee.count

import com.lee.dao.FileDao
import org.apache.commons.lang.StringUtils

import scala.io.Source

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/30
  * Time: 10:07
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object GetFilterFeature {
  def main(args: Array[String]): Unit = {
    val filter = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\res0.txt").getLines().map(line => {
      val split = line.substring(1, line.length - 1).split(",")
      (split(0), split(4))
    }).toList.filter(_._2.toDouble > 0.7).map(_._1)
    FileDao.saveString("C:\\Users\\wei_z\\Desktop\\分析\\filter.txt",filter.mkString("\n"))
  }
}
