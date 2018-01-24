package com.lee.test
import scala.collection.immutable.Seq

/**
  * Created with Lee.
  * User: root
  * Date: 2018/1/15
  * Time: 9:52
  * To change this template use File | Settings | File Templates.
  * Description: 
  */

object Test {


  implicit def Object2SpecialPerson(obj:Object):SpecialPerson ={
    if(obj.isInstanceOf[Older]){
      val older = obj.asInstanceOf[Older]
      new SpecialPerson(older.name)
    }else if(obj.isInstanceOf[Child]){
      val child = obj.asInstanceOf[Child]
      new SpecialPerson(child.name)
    }else{
      null
    }
  }
  var sumTickets  = 0
  def buySpecialTicket(s : SpecialPerson) = {
    sumTickets += 1
    println(sumTickets)
  }

  def main(args: Array[String]): Unit = {
    val test:AnyRef = new Older("")
    buySpecialTicket(test)
    val ints: Seq[Int] = 1::2::3::Nil

  }
}
case class SpecialPerson(name:String)
case class Older(val name:String)
case class Child(val name:String)
