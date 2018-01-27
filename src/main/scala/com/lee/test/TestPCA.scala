package com.lee.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/26
  * Time: 14:57
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object TestPCA {
  def main(args: Array[String]): Unit = {
   /* if (args.length != 1) {
      System.err.println("Usage: TallSkinnyPCA <input>")
      System.exit(1)
    }*/

    val conf = new SparkConf().setAppName("TallSkinnyPCA")
    val sc = new SparkContext(conf)

    // Load and parse the data file.
    val rows = sc.textFile("C:\\Users\\wei_z\\Desktop\\dxtrait\\pca.txt").map { line =>
      val values = line.split(' ').map(_.toDouble)
      Vectors.dense(values)
    }
    val mat = new RowMatrix(rows)

    // Compute principal components.
    val pc = mat.computePrincipalComponents(mat.numCols().toInt)

    println("Principal components are:\n" + pc)

    sc.stop()
  }
}
