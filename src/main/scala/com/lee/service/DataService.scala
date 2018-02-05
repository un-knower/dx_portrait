package com.lee.service

import java.io.PrintWriter

import com.lee.dao.FileDao
import com.lee.des.DesUtil
import com.lee.main.JobArgs
import com.lee.utils._
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
      mongoOrginRdd.coalesce(200).saveAsObjectFile(PathUtil.getMongoOrginPath)
    } else {
      log.warn("mongo orgin has exists")
    }
    if (!HDFSUtil.exists(PathUtil.getMongoSampleDataPath)) {
      log.warn("deal today mongo sample data ")
      //广播app对应的分类
      val app_class_name_brod = sc.broadcast(FIles.app_class_name)
      val app_name_broad = sc.broadcast(FIles.app_name)
      //是否存在原始数据
      val rdd = sc.objectFile[BSONObject](PathUtil.getMongoOrginPath)
      val saveRdd = rdd.filter(bson => {
        val list = bson.get("apps").asInstanceOf[BasicDBList]
        list.size() >= 2
      }).map(bson => {
        try {
          val app_class_name = app_class_name_brod.value
          val app_name = app_name_broad.value
          val stringBuilder = new StringBuilder
          //把一跳数据处理成一个样本保存
          val oneList = new ListBuffer[(String, Double)]
          bson.get("apps").asInstanceOf[BasicDBList].foreach(line => {
            val app = line.asInstanceOf[BasicBSONObject]
            //app
            val app_id = app.getString("k")
            val atime = app.getOrDefault("atime", "2000-01-01").toString
            if (atime >= DateUtil.getDateStr(-10, "yyyy-MM-dd")) {
              //频次
              var app_freq: Double = 1
              if (app.containsField("v")) {
                app_freq = app.getDouble("v")
              }
              oneList += ((app_id, app_freq))
              //标签
              var tags: List[String] = List[String]()
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
          stringBuilder.append(res.mkString("|") + ",")
          //增加_id 输出
          stringBuilder.append(bson.get("_id").toString)
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
        if (split.length == 4) {
          if (split(2).split("\\|").length >= 2) {
            if (StringUtils.isNotBlank(split(0)) && StringUtils.isNotBlank(split(1)) && StringUtils.isNotBlank(split(2)) && StringUtils.isNotBlank(split(3))) {
              if (split(0).equals("男") || split(0).equals("女")) {
                if (split(1).length == 8) {
                  val i = 2018 - split(1).substring(0, 4).toInt
                  if (i <= 60 && i >= 12) {
                    flag = true
                  }
                }
              }
            }
          }
        }
        flag
      }).repartition(PropUtil.prop.getProperty("repartition.num", 20 + "").toInt)
      getMongo()
      mongoRdd.saveAsTextFile(PathUtil.getMongoSampleDataPath)
    } else {
      log.warn("mongo data has exists")
    }


  }


  def takeFeature2File = {
    if (!HDFSUtil.exists(PathUtil.getFeatureIndex)) {
      log.warn("start take feature")
      if (mongoRdd == null) {
        mongoRdd = sc.textFile(PathUtil.getMongoSampleDataPath)
        mongoRdd = mongoRdd.filter(line => {
          val split = line.split(",")
          split.length == 4 && StringUtils.isNotBlank(split(2))
        })
        mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      //文件提取特征
      val feature_index = mongoRdd.flatMap(line => {
        val features = line.split(",")(2)
        features.split("\\|").map(line => line.split(":")(0))
      }).distinct().collect().sorted.zipWithIndex.map(line => (line._1, line._2 + 1))
      FileDao.saveFeatureIndex(feature_index, PathUtil.getFeatureIndex)

    } else {
      log.warn("feature index has exists")
    }

  }


  def countSample(): Unit = {
    if (JobArgs.sampleCount.equals("1")) {
      log.warn("count sample data")
      if (mongoRdd == null) {
        mongoRdd = sc.textFile(PathUtil.getMongoSampleDataPath)
        mongoRdd = mongoRdd.filter(line => {
          val split = line.split(",")
          split.length == 4 && StringUtils.isNotBlank(split(2))
        })
        mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      val count = mongoRdd.flatMap(line => {
        val split = line.split(",")
        split(2).split("\\|").map(line => (line.split(":")(0), 1))
      }).reduceByKey(_ + _).collect().sortBy(-_._2)
      FileDao.saveSampleCountFile(count, PathUtil.getSampleCountPath("samplecount.txt"))
      //统计男女  年龄
      val sexCount = mongoRdd.map(line => {
        val split = line.split(",")
        (split(0), 1)
      }).reduceByKey(_ + _).collect().sortBy(-_._2)
      FileDao.saveSampleCountFile(sexCount, PathUtil.getSampleCountPath("samplesexcount.txt"))
      //年龄统计
      val ageCount = mongoRdd.map(line => {
        val split = line.split(",")
        if (split(1).length >= 4) {
          (2018 - split(1).substring(0, 4).toInt + "", 1)
        } else {
          (split(1), 1)
        }
      }).reduceByKey(_ + _).collect().sortBy(-_._2)
      FileDao.saveSampleCountFile(ageCount, PathUtil.getSampleCountPath("sampleagecount.txt"))
    }
  }


  def countSVM(): Unit = {
    if (JobArgs.sampleCount.equals("1")) {
      log.warn("count svm data")
      if (mongoRdd == null) {
        mongoRdd = sc.textFile(PathUtil.getMongoSampleDataPath)
        mongoRdd = mongoRdd.filter(line => {
          val split = line.split(",")
          split.length == 4 && StringUtils.isNotBlank(split(2))
        })
        mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
      }
      val svmRdd = sc.textFile(PathUtil.getSvmSavePath)
      val svmCount = svmRdd.map(line => {
        (line.split(" ")(0), 1)
      }).reduceByKey(_ + _).collect()
      FileDao.saveSampleCountFile(svmCount, PathUtil.getSampleCountPath(JobArgs.modelName + "_svmcount.txt"))
    }
  }

  def dealData2Svm(): Unit = {
    val feature_index_map: mutable.HashMap[String, String] = FileDao.readFeatureIndex2Map(PathUtil.getFeatureIndex)
    val feature_index_map_broad = sc.broadcast(feature_index_map)
    //增加feature过滤
    val feature_filter = new mutable.HashSet[String]()
    if (HDFSUtil.exists(PathUtil.configPathByName("filter_feature.txt"))) {
      log.warn("read filter feature")
      feature_filter ++= FileDao.getFilterFeature(PathUtil.configPathByName("filter_feature.txt"))
    }
    val feature_filter_broad = sc.broadcast(feature_filter)
    if (!HDFSUtil.exists(PathUtil.getSvmSavePath)) {
      log.warn("start deal sample 2 svm")
      //读取训练样本
      val broadcast = sc.broadcast(JobArgs.modelName)
      //提取样本特征 处理成索引
      var svmRdd: RDD[String] = mongoRdd.mapPartitions(iterable => {
        val feature_filter = feature_filter_broad.value
        val sex_age = broadcast.value
        val map = feature_index_map_broad.value
        val list = new ListBuffer[String]
        //性别 split 特征
        val stringBuilder: StringBuilder = new StringBuilder
        var next: String = null
        var split: Array[String] = null
        var sex: String = null
        var age: String = null
        var feature: String = null
        while (iterable.hasNext) {
          next = iterable.next()
          split = next.split(",")
          if (sex_age.equals("sex")) {
            sex = if (split(0).equals("男")) "0"
            else "1"
            stringBuilder.append(sex + " ")
          } else {
            age = 2018 - split(1).substring(0, 4).toInt + ""
            stringBuilder.append(age + " ")
          }
          //特征
          feature = split(2)
          var feature_weights = feature.split("\\|").map(line => {
            val strs = line.split(":")
            (strs(0), strs(1))
          })
          //如果没用特征过滤
          if (feature_filter.size != 0) {
            feature_weights = feature_weights.filter(line => {
//              feature_filter.contains(line._1) || feature_filter.contains(line._1.split("_")(0))
              feature_filter.contains(line._1)
            })
          }
          //取特征索引
          val res_feature = feature_weights.map(line => {
            (map.getOrElse(line._1, null), line._2)
          }).filter(_._1 != null).sortBy(_._1.toDouble).map(line => line._1 + ":" + line._2)
          res_feature

          if (res_feature.size >= 3) {
            stringBuilder.append(res_feature.mkString(" "))
            list += stringBuilder.toString()
          }
          stringBuilder.clear()
        }
        list.iterator
      })

      if (JobArgs.sampleTake != null) {
        //过滤男样本10W个
        val sort = svmRdd.map(line => {
          (line, line.split(" ").length)
        }).sortBy(-_._2)
        val count = JobArgs.sampleTake.toInt
        val pSample = sort.filter(line => {
          line._1.split(" ")(0).equals("1")
        }).zipWithIndex().filter(_._2 < count)
        val nSample = sort.filter(line => {
          line._1.split(" ")(0).equals("0")
        }).zipWithIndex().filter(_._2 < count)
        svmRdd = pSample.union(nSample).map(_._1._1)
      }
      //过滤男样本10W个
      svmRdd.repartition(20).saveAsTextFile(PathUtil.getSvmSavePath)
    } else {
      log.warn("svm has exist")
    }

  }

  def run(): Unit = {
    getSampleRdd
    getMongo
    takeFeature2File
    countSample
    dealData2Svm
    countSVM
  }

  def getMongo(): Unit = {
    if (mongoRdd == null) {
      mongoRdd = sc.textFile(PathUtil.getMongoSampleDataPath)
      mongoRdd = mongoRdd.filter(line => {
        val split = line.split(",")
        split.length == 4 && StringUtils.isNotBlank(split(2)) && split(2).split("\\|").length >= 2
      })
      mongoRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    }
  }
}
