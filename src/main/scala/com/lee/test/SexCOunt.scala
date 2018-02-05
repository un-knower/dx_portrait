package com.lee.test

import com.lee.dao.FileDao
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/25
  * Time: 17:56
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object SexCOunt {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.textFile("hdfs://master:8020/user/lihw/output/hostsex2/")
    rdd.cache()
    /*val count = rdd.count()
    println(count)
    val count1 = rdd.filter(line => {
      val split = line.split(",")
      split(split.length-1).toInt > 1
    }).count()*/
    val collect = rdd.filter(line => {
      val host_sex = line.substring(0, line.lastIndexOf(","))
      val sex = host_sex.substring(host_sex.lastIndexOf("_") + 1)
      sex.equals("男") || sex.equals("女")
    }).map(line => {
      //host
      val host_sex = line.substring(0, line.lastIndexOf(","))
      val count = line.substring(line.lastIndexOf(",") + 1)
      val host = host_sex.substring(0, host_sex.lastIndexOf("_"))
      val sex = host_sex.substring(host_sex.lastIndexOf("_") + 1)
      (host, sex + "_" + count)
    }).groupByKey().map(tup => {
      val host = tup._1
      val sex = tup._2.mkString("&")

      //总数
      var sum = 0
      val map = tup._2.map(line => {
        val split = line.split("_")
        sum += split(1).toInt
        (split(0), split(1))
      }).toMap
      //算出男女百分比
      var xm = 0.0
      var xf = 0.0
      xm = 1.0 * map.getOrElse("男", "1").toInt / sum
      xf = 1.0 * map.getOrElse("女", "1").toInt / sum
      val cha = (xm - xf).abs
      (host, sex, xm, xf, cha ,sum)
    }).filter(_._6 > 5000).collect()
    val save = collect.take(6474).map(line => {
      line._1
    }).zipWithIndex.map(line => "NB" + (line._2+1) + "," + line._1+"/*")
    save
    FileDao.saveString("C:\\Users\\wei_z\\Desktop\\index_host.csv", save.mkString("\n"))
    /* val getCount  = collect.size * 0.3
     val round = getCount.round
     val xf = collect.sortBy(-_._3).take(round.toInt)
     FileDao.saveString("C:\\Users\\wei_z\\Desktop\\分析\\xf.txt",xf.mkString("\n"))*/

  }
}
