package com.lee.utils

import java.io.InputStream
import java.util.Properties

/**
  * Created by root on 2017/7/31.
  */
object PropUtil {
  var prop: Properties = new Properties()
  prop.load(getClass.getClassLoader.getResourceAsStream("local.properties"))

  def init(name: String): Unit = {
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream(name)
    prop.load(inputStream)
  }

  def getProperty(key: String): String = prop.getProperty(key)

  def getInt(key: String): Int = prop.getProperty(key).toInt

  def getBoolean(key: String): Boolean = prop.getProperty(key).toBoolean

  def getDouble(key: String): Double = prop.getProperty(key).toDouble

  def getLong(key: String): Long = prop.getProperty(key).toLong

  def getPropByName(name: String) = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream(name))
    properties
  }
}
