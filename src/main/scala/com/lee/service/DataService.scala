package com.lee.service

import java.io.PrintWriter

import com.lee.dao.FileDao
import com.lee.des.DesUtil
import com.lee.main.JobArgs
import com.lee.utils.{FIles, HDFSUtil, PathUtil, PropUtil}
import com.mongodb.BasicDBList
import com.mongodb.hadoop.MongoInputFormat
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bson.{BSONObject, BasicBSONObject}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/15
  * Time: 10:57
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object DataService extends Serializable {
  var sc: SparkContext = _
  var mongoRdd: RDD[String] = _
  val log = Logger.getLogger(getClass)

  //TODO 初始化数据服务
  def init(sparkContext: SparkContext): Unit = {
    sc = sparkContext
  }

  def getFileld(s: String, others: BasicBSONObject) = {
    if (others.containsField(s)) {
      val list = others.get(s).asInstanceOf[BasicDBList]
      val sex = list.get(0).asInstanceOf[BasicBSONObject]
      val k = sex.getString("k")
      DesUtil.decode(k)
    } else {
      "null"
    }
  }

  def getAge(g: String) = {
    2018 - g.substring(0, 4).toInt
  }

  def getSampleRdd: Unit = {
    val config = new Configuration()
    PropUtil.prop.foreach(line => {
      if (line._1.startsWith("mongo.input")) {
        config.set(line._1, line._2)
      }
    })
    if (!HDFSUtil.exists(PathUtil.getMongoOrginPath)) {
      log.warn("deal mongo orgin data")
      val mongo = sc.newAPIHadoopRDD(config, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
      val mongoOrginRdd = mongo.map(_._2)
      mongoOrginRdd.saveAsObjectFile(PathUtil.getMongoOrginPath)
    }else{
      log.warn("mongo orgin has exists")
    }
    if (!HDFSUtil.exists(PathUtil.getMongoSampleDataPath)) {
      log.warn("deal today mongo sample data ")
      //广播app对应的分类
      val app_class_name_brod = sc.broadcast(FIles.app_class_name)
      val app_name_broad = sc.broadcast(FIles.app_name)
      //是否存在原始数据
      val rdd = sc.objectFile[BSONObject](PathUtil.getMongoOrginPath)
      val saveRdd = rdd.map(bson => {
        try {
          val app_class_name = app_class_name_brod.value
          val app_name = app_name_broad.value
          val stringBuilder = new StringBuilder
          //把一跳数据处理成一个样本保存
          val oneList = new ListBuffer[(String, Double)]
          bson.get("apps").asInstanceOf[BasicDBList].foreach(line => {
            breakable {
              val app = line.asInstanceOf[BasicBSONObject]
              //app
              val app_id = app.getString("k")
              /*if (!app_name.contains(app_id)) {
                break()
              }*/
              //频次
              var app_freq: Double = 1
              if (app.containsField("v")) {
                app_freq = app.getDouble("v")
              }
              oneList += ((app_id, app_freq))
              //标签
              var tags: List[String] = null
              if (app_class_name.contains(app_id)) {
                tags = app_class_name.get(app_id).get.map(_._1).toList
                tags.foreach(line => oneList += ((line, app_freq)))
              }
              //t_t
              if (app.containsField("t_t")) {
                val split = app.getString("t_t").split("_")
                val d = split(0)
                val h = split(1)
                //app tag  与天的组合
                oneList += ((app_id + "_d" + d, 1))
                tags.foreach(tag => oneList += ((tag + "_d" + d, 1)))
                //处理小时  00000 0000 0000 00 0000 01000
                val h0 = h.substring(0, 5)
                val h1 = h.substring(5, 9)
                val h2 = h.substring(9, 13)
                val h3 = h.substring(13, 15)
                val h4 = h.substring(15, 19)
                val h5 = h.substring(19, 24)
                var tuples = (h0, "h0-5") :: (h1, "h5-9") :: (h2, "h9-13") :: (h3, "h13-15") :: (h4, "h15-19") :: (h5, "h19-24") :: Nil
                tuples = tuples.filter(tup => {
                  tup._1.contains("1")
                })
                //app tag 时间点组合
                tuples.foreach(tup => {
                  oneList += ((app_id + "_" + tup._2, StringUtils.countMatches(tup._1, "1")))
                })
                tags.foreach(tag => {
                  tuples.foreach(tup => {
                    oneList += ((tag + "_" + tup._2, StringUtils.countMatches(tup._1, "1")))
                  })
                })
              }
            }
          })
          //手机型号
          if (bson.containsField("m")) {
            oneList += ((bson.get("m").toString, 1))
          }
          //如果有品牌 也作为一个特征
          if (bson.containsField("b")) {
            oneList += ((bson.get("m").toString, 1))
          }
          //如果又相同的聚合
          val res = oneList.groupBy(_._1).map(line => {
            line._1 + ":" + line._2.map(_._2).reduce(_ + _)
          })
          //抽取性别和年龄
          val others = bson.get("others").asInstanceOf[BasicBSONObject]
          val x = getFileld("x", others)
          if (x.equals("null")) {
            stringBuilder.append(x + ",")
          } else {
            stringBuilder.append(x + ",")
          }
          val g = getFileld("g", others)
          if (!g.equals("null")) {
            stringBuilder.append(g + ",")
          } else {
            stringBuilder.append(g + ",")
          }
          stringBuilder.append(res.mkString("|"))
          stringBuilder.toString()
        } catch {
          case e: Exception => e.printStackTrace()
            log.error("fail bson is " + bson.toString)
            "null"
        }
      })
      mongoRdd = saveRdd.filter(line => {
        var flag = false
        val split = line.split(",")
        if (split.length == 3) {
          if (split(0).equals("男") || split(0).equals("女")) {
            if (split(1).length == 8) {
              val i = 2018 - split(1).substring(0, 4).toInt
              if (i <= 60 && i >= 12) {
                flag = true
              }
            }
          }
        }
        flag
      }).repartition(PropUtil.prop.getProperty("repartition.num", 20 + "").toInt)
      mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      mongoRdd.saveAsTextFile(PathUtil.getMongoSampleDataPath)
    } else {
      log.warn("taday has deal mongo data")
    }
  }


  //TODO 提取特征和索引
  def takeFeature2File = {
    if (!HDFSUtil.exists(PathUtil.getFeatureIndex)) {
      if (mongoRdd == null) {
        mongoRdd = sc.textFile(PathUtil.getMongoSampleDataPath)
        mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      //文件提取特征
      val feature_index = mongoRdd.filter(line => {
        line.split(",").length == 3
      }).flatMap(line => {
        val features = line.split(",")(2)
        features.split("\\|").map(line => line.split(":")(0))
      }).distinct().collect().zipWithIndex
      FileDao.saveFeatureIndex(feature_index, PathUtil.getFeatureIndex)
      log.warn("take feature index success")
    } else {
      log.warn("feature index has exists")
    }

  }


  //TODO 样本转为svm数据
  def Data2Svm: Unit = {
    if (!HDFSUtil.exists(PathUtil.getSvmSavePath)) {
      log.warn("start deal sample 2 svm")
      //广播特征索引
      val feature_index_map: mutable.HashMap[String, String] = FileDao.readFeatureIndex2Map(PathUtil.getFeatureIndex)
      val feature_index_map_broad = sc.broadcast(feature_index_map)
      //读取训练样本
      if (mongoRdd == null) {
        mongoRdd = sc.textFile(PathUtil.getMongoSampleDataPath)
        mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      //提取样本特征 处理成索引
      mongoRdd.mapPartitions(iterable => {
        val map = feature_index_map_broad.value
        val list = new ListBuffer[String]
        //性别 split 特征
        val stringBuilder: mutable.StringBuilder = new StringBuilder
        var next: String = null
        var split: Array[String] = null
        var sex: String = null
        var feature: String = null
        while (iterable.hasNext) {
          next = iterable.next()
          split = next.split(",")
          sex = if (split(0).equals("男")) "0"
          else "1"
          stringBuilder.append(sex + " ")
          //特征
          feature = split(2)
          feature = feature.split("\\|").map(line => {
            val split1 = line.split(":")
            (map.getOrElse(split1(0), null), split1(1))
          }).filter(_._1 != null).map(line => line._1 + ":" + line._2).mkString(" ")
          stringBuilder.append(feature)
          list += stringBuilder.toString()
          stringBuilder.clear()
        }
        list.iterator
      }).saveAsTextFile(PathUtil.getSvmSavePath)
    } else {
      log.warn("svm has exist")
    }
  }

  def countSample(): Unit = {
    if (JobArgs.sampleCount.equals("1")) {
      log.warn("count sample data")
      if (mongoRdd == null) {
        mongoRdd = sc.textFile(PathUtil.getMongoSampleDataPath)
        mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      val count = mongoRdd.filter(line => {
        line.split(",").length == 3
      }).flatMap(line => {
        val split = line.split(",")
        split(2).split("\\|").map(line => (line.split(":")(0), 1))
      }).reduceByKey(_ + _).collect().sortBy(-_._2)
      FileDao.saveSampleCountFile(count, PathUtil.getSampleCountPath("samplecount.txt"))
      //统计男女  年龄
      val sexCount = mongoRdd.map(line => {
        val split = line.split(",")
        (split(0), 1)
      }).reduceByKey(_ + _).collect().sortBy(-_._2)
      FileDao.saveSampleCountFile(sexCount, PathUtil.getSampleCountPath("sexcount.txt"))
      //年龄统计
      val ageCount = mongoRdd.map(line => {
        val split = line.split(",")
        if (split(1).length >= 4) {
          (2018 - split(1).substring(0, 4).toInt + "", 1)
        } else {
          (split(1), 1)
        }
      }).reduceByKey(_ + _).collect().sortBy(-_._2)
      FileDao.saveSampleCountFile(ageCount, PathUtil.getSampleCountPath("agecount.txt"))
    }
  }

  def run(): Unit = {
    if (HDFSUtil.exists(PathUtil.getSvmSavePath) && HDFSUtil.exists(PathUtil.getFeatureIndex)) {
      log.info("svm data and feature index has exists , end")
    } else {
      getSampleRdd
      takeFeature2File
      countSample
      Data2Svm
    }
  }
}
