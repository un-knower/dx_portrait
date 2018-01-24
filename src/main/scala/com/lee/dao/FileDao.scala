package com.lee.dao

import java.io.PrintWriter

import com.lee.utils.{HDFSUtil, PathUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader

import scala.util.control.Breaks._
import scala.collection.mutable

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/15
  * Time: 14:28
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object FileDao {
  def saveSampleCountFile(count: Array[(String, Int)], getSampleCountPath: String) = {
    val writer = new PrintWriter(HDFSUtil.getOutout(getSampleCountPath, true))
    count.foreach(line => writer.println(line._1 + " " + line._2))
    writer.flush()
    writer.close()
  }

  def readFeatureIndex2Map(path: String) = {
    HDFSUtil.readHdfs2Map(path," ")
  }

  def saveFeatureIndex(feature_index: Array[(String, Int)], getFeatureIndex: String) = {
    val writer = new PrintWriter(HDFSUtil.getOutout(PathUtil.getFeatureIndex, false))
    feature_index.foreach(line => writer.println(line._1 + " " + line._2))
    writer.flush()
    writer.close()
  }



  def getAppFile2Map(path: String) = {
    HDFSUtil.readHdfs2Map(path)
  }

  def getAppClassName(path: String) = {
    val map = new mutable.HashMap[String, mutable.HashMap[String, String]]()
    var lineReader: LineReader = null
    val text = new Text()

    var split: Array[String] = null
    var col1 = ""
    var col2 = ""
    var col3 = ""

    try {
      lineReader = new LineReader(HDFSUtil.openFile(path))
      while (lineReader.readLine(text) > 0) {
        breakable {
          val line = text.toString
          split = line.split(",")
          if (split.length < 3) {
            break()
          }
          col1 = split(0)
          col2 = split(1)
          col3 = split(2)
          if (map.contains(col1)) {
            map.get(col1).get.put(col2,col3)
          }else{
            val innerMap = new mutable.HashMap[String,String]()
            innerMap.put(col2,col3)
            map.put(col1,innerMap)
          }
        }
      }
    }
    map
  }


}
