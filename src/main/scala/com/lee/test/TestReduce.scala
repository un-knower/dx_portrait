package com.lee.test

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/23
  * Time: 9:28
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object TestReduce {
  def main(args: Array[String]): Unit = {
    val tuples = ("n1",1) :: ("n1",2) :: ("n2",1) :: Nil
    tuples.groupBy(_._1).map(line=>{
      line._2.map(_._2).reduce(_+_)
    }).foreach(println)
  }
}
