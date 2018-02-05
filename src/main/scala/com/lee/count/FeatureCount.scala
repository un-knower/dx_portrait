package com.lee.count

import java.io.FileWriter

import scala.io.Source

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/27
  * Time: 15:06
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object FeatureCount {
  def main(args: Array[String]): Unit = {
    val weights = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\weights.txt")
    val weight = weights.mkString("").split(",")

    val feature_index = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\20180129_feature_index.txt")
    val top_weight = feature_index.getLines().map(line => {
      line.split(",")(0)
    }).zip(weight.iterator).toList.sortBy(-_._2.toDouble.abs)

    val tagappname = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\tagappname")
    val tag_name = tagappname.getLines().map(line => {
      val split = line.split(",")
      (split(1), split(2))
    }).toMap
    tag_name
    val app_name_effect = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\app_name_effect")
    val app_name = app_name_effect.getLines().map(line => {
      val split = line.split(",")
      (split(0), split(1))
    }).toMap

    println(app_name.contains("AP2653")  )

    val map = top_weight.map(line => {

      if (app_name.contains(line._1)) {
        (line, app_name.get(line._1).get)
      } else if (tag_name.contains(line._1)) {
        (line, tag_name.get(line._1).get)
      } else if (line._1.contains("_")) {
        val split = line._1.split("_")
        val v1 = split(0)
        val v2 = split(1)
        if (app_name.contains(v1)) {
          (line, app_name.get(v1).get)
        } else if (tag_name.contains(v1)) {
          (line, tag_name.get(v1).get)
        } else {
          (line, "null")
        }
      } else {
        (line, "null")
      }
    }).map(line => {
      line._1._1 + "," + line._1._2 + "," + line._2
    })

    val writer = new FileWriter("C:\\Users\\wei_z\\Desktop\\分析\\权重分析.txt", true)
    writer.write(map.mkString("\n"))
    writer.flush()
    writer.close()
  }
}
