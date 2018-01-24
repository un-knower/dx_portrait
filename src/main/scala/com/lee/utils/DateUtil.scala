package com.lee.utils

import java.text.{DecimalFormat, SimpleDateFormat}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.{Calendar, Date}

/**
  * Created by root on 2017/8/18.
  */
object DateUtil {

  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date = dateFormat.format(now)
    date
  }
  def getYesterday(): String ={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    var yesterday=dateFormat.format(cal.getTime())
    yesterday
  }

  def getNowWeekStart():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period=df.format(cal.getTime())
    period
  }

  def getNowWeekEnd():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);//这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1)// 增加一个星期，才是我们中国人的本周日的日期
    period=df.format(cal.getTime())
    period
  }

  def getNowMonthStart():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    period=df.format(cal.getTime())//本月第一天
    period
  }

  def getNowMonthEnd():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    period=df.format(cal.getTime())//本月最后一天
    period
  }

  def DateFormat(time:String):String={
    var sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date:String = sdf.format(new Date((time.toLong*1000l)))
    date
  }

  def timeFormat(time:String):String={
    var sdf:SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
    var date:String = sdf.format(new Date((time.toLong*1000l)))
    date
  }

  def getCoreTime(start_time:String,end_Time:String)={
    var df:SimpleDateFormat=new SimpleDateFormat("HH:mm:ss")
    var begin:Date=df.parse(start_time)
    var end:Date = df.parse(end_Time)
    var between:Long=(end.getTime()-begin.getTime())/1000//转化成秒
    var hour:Float=between.toFloat/3600
    var decf:DecimalFormat=new DecimalFormat("#.00")
    decf.format(hour)//格式化
  }

  def getCurMinStr(format:String): String ={
    val pattern = DateTimeFormatter.ofPattern(format)
    val now = LocalDateTime.now()
    now.format(pattern)
  }

  def getDateStr(day:Int): String = {
    val now = LocalDateTime.now()
    DateTimeFormatter.ofPattern("yyyyMMdd").format(now.plusDays(day))
  }

  def getDateInterval(strDate1:String,strDate2:String): Int ={
    val date1 = LocalDate.parse(strDate1, DateTimeFormatter.ofPattern("yyyyMMdd"))
    val date2 = LocalDate.parse(strDate2, DateTimeFormatter.ofPattern("yyyyMMdd"))
    val between = date2.toEpochDay - date1.toEpochDay
    between.toInt
  }

  def getDateStr(baseDate:String,day:Int): String = {
    val now = LocalDate.parse(baseDate,DateTimeFormatter.ofPattern("yyyyMMdd"))
    DateTimeFormatter.ofPattern("yyyyMMdd").format(now.plusDays(day))
  }

  def getCurMinStr(): String ={
    val pattern = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
    val now = LocalDateTime.now()
    now.format(pattern)
  }

  def getCurrentDateStr(): String ={
    getDateStr(0)
  }

  def dateCompare(date1:String,date2:String): Int = {
    val d1 = LocalDate.parse(date1,DateTimeFormatter.ofPattern("yyyyMMdd"))
    val d2 = LocalDate.parse(date2,DateTimeFormatter.ofPattern("yyyyMMdd"))
    if (d1.isAfter(d2)) 1
    else if (d1.isBefore(d2))       -1
    else 0
  }

  def getCurTimeStr(): String ={
    val pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val now = LocalDateTime.now()
    now.format(pattern)
  }

  def getCurDataHourStr(): String ={
    getCurDataTimeByPattern("yyyyMMddHH")
  }

  def getCurDataTimeByPattern(pat :String): String ={
    val pattern = DateTimeFormatter.ofPattern(pat)
    val now = LocalDateTime.now()
    now.format(pattern)
  }

  def main(args: Array[String]): Unit = {
    println(getDateStr(1))
  }
}
