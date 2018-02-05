package com.lee.count

import com.lee.utils.MllibUtils
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/29
  * Time: 9:58
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object SampleCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("SampleCount")
    val sc = new SparkContext(conf)
    val sample = sc.textFile("hdfs://master:8020/user/lihw/data/trainsample/20180129")
    //广播索引还有 app tag对应的名字
    val tagappname = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\tagappname")
    val tag_name = tagappname.getLines().map(line => {
      val split = line.split(",")
      (split(1), split(2))
    }).toMap
    val tag_name_broad = sc.broadcast(tag_name)
    val app_name_effect = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\app_name_effect")
    val app_name = app_name_effect.getLines().map(line => {
      val split = line.split(",")
      (split(0), split(1))
    }).toMap
    val app_name_broad = sc.broadcast(app_name)
    //C:\Users\wei_z\Desktop\分析
    val feature_index_txt = Source.fromFile("C:\\Users\\wei_z\\Desktop\\分析\\20180129_feature_index.txt")
    val feature_index = feature_index_txt.getLines().map(line => {
      val split = line.split(",")
      (split(0),split(1))
    }).toMap
     val feature_index_broad = sc.broadcast(feature_index)
     //广播模型
     val model = LogisticRegressionModel.load(sc,"hdfs://master:8020/user/lihw/output/model/sex/LBFGS/201801270947")
     val model_broad = sc.broadcast(model)
     //处理成labelpoint去预测
    val map = sample.map(line => {
      val app_name = app_name_broad.value
      val tag_name = tag_name_broad.value
      val model = model_broad.value
      val feature_index = feature_index_broad.value
      val Array(sex, age, feature, _id) = line.split(",")
      var label = 0
      //处理成labelpoint
      if (sex.equals("女")) label = 1
      val stringBuilder = new StringBuilder
      val (indices, values) = feature.split("\\|").map(line => {
        val Array(feature, value) = line.split(":")
        //得到中文
        if (app_name.contains(feature)) {
          stringBuilder.append(app_name.get(feature).get+"|")
        }else if (tag_name.contains(feature)){
          stringBuilder.append(tag_name.get(feature).get+"|")
        }else if(feature.contains("_")){
          val split= feature.split("_")
          val v1 = split(0)
          val v2 = split(1)
          if (app_name.contains(v1)) {
            stringBuilder.append(app_name.get(v1).get+"_"+v2+"|")
          }else if (tag_name.contains(v1)) {
            stringBuilder.append(tag_name.get(v1).get+"_"+v2+"|")
          }else{
            stringBuilder.append(feature+"|")
          }
        } else{
          stringBuilder.append(feature+"|")
        }
        (feature_index.getOrElse(feature, 0).toString.toInt-1, value.toDouble)
      }).sortBy(_._1).unzip
      val numFeatures = feature_index.size
      val sparse: Vector = Vectors.sparse(numFeatures, indices.toArray, values.toArray)
      //开始预测
      val pre = if (model.predict(sparse).toInt == 0) "男" else "女"
      "预测值："+pre+",实际值："+sex+","+stringBuilder.toString()+","+_id
    }).saveAsTextFile("hdfs://master:8020/user/lihw/output/samplecount/")


  }
}
