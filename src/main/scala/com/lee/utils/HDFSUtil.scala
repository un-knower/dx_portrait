package com.lee.utils

import java.io._
import java.net.URI
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONReader
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.mortbay.util.ajax.JSON

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by root on 2017/8/10.
  */
object HDFSUtil {
  val configuration = new Configuration()
  var hdfs: FileSystem = null
  val log = Logger.getLogger(getClass)

  def init(defaultName: String): Unit = {
    if (hdfs == null && defaultName != null) {
      configuration.set("fs.defaultFS", defaultName)
    }
    hdfs = FileSystem.get(configuration)
    log.info("hdfs初始化成功")
  }

  def setconf(conf: Configuration): Unit = {
    hdfs = FileSystem.get(conf)
    log.info("hdfs初始化成功")
  }


  def openFile(filePath: String): FSDataInputStream = {
    if (!exists(filePath)) {
      log.error("HDFSUitl.java getFile failed ,caused by the file: " + filePath + " is not exists.")
      throw new IllegalArgumentException("file not exists")
    } else {
      hdfs.open(new Path(filePath))
    }
  }

  /**
    * 判断HDFS上一个文件是否存在
    *
    * @param fileName
    * 文件名
    * @return 是否存在
    */
  def exists(fileName: String): Boolean = {
    if (StringUtils.isEmpty(fileName)) {
      return false
    } else {
      if (hdfs == null) throw new IllegalArgumentException("must call init hdfs method first")
      try {
        hdfs.exists(new Path(fileName))
      } catch {
        case e: IOException => {
          log.error("HDFSUitl exists error. " + e.getMessage)
          false
        }
      }
    }
  }

  /**
    * 加载hdfs配置
    * @param filePath
    * @param prop
    * @return
    */
  def getOutConfig(filePath: String, prop: Properties): Properties = {
    var dis: FSDataInputStream = null
    try {
      dis = hdfs.open(new Path(filePath))
      prop.load(dis)
      prop
    } catch {
      case e: Exception =>
        log.error("getOutConfig is fail job exit")
        e.printStackTrace()
        prop
    }
  }

  /**
    * 写日志到hdfs
    * @param filePath
    * @param info
    */
  def writeStr2Hdfs(filePath: String, info: String): Unit = {
    var writer: PrintWriter = null
    try {
      if (exists(filePath)) {
        writer = new PrintWriter(hdfs.append(new Path(filePath)))
      } else {
        writer = new PrintWriter(hdfs.create(new Path(filePath), false))
      }
      if (info != null) {
        writer.println(info)
      }
      writer.flush()
    } catch {
      case e: Exception => log.error(e.printStackTrace())
    } finally {
      writer.close()
    }
  }

  def getWriter(filePath: String,over: Boolean) = {
      new PrintWriter(hdfs.create(new Path(filePath), over))
  }

  /**
    * 目录文件列表
    *
    * @param path 目录路径
    * @return
    */
  def getFiles(path: String): ListBuffer[String] = {
    val listBUffer = new ListBuffer[String]()
    try {
      if (!hdfs.exists(new Path(path))) {
        log.error(path + "目录不存在")
      } else {
        val fileStatus = hdfs.listStatus(new Path(path))
        if (fileStatus != null) {
          for (i <- 0 until fileStatus.length) {
            if (!fileStatus(i).isDirectory) {
              listBUffer += fileStatus(i).getPath.toString
            }
          }
        }
      }
    } catch {
      case i: IllegalArgumentException => new IllegalArgumentException("hdfs未初始化，或者初始化失败")
      case e: Exception => e.printStackTrace()
    }
    listBUffer
  }

  /**
    * 删除文件
    *
    * @param filePath  文件路径
    * @param recursive 递归删除目录
    */
  def deleteFile(filePath: String, recursive: Boolean): Unit = {
    if (filePath == null || filePath == "") {
      log.warn(s"$filePath is illegal")
    }
    try {
      hdfs.delete(new Path(filePath), recursive)
      log.info(s"$filePath delete success and recursive is true")
    } catch {
      case i: IllegalArgumentException => new IllegalArgumentException("hdfs未初始化，或者初始化失败")
      case e: Exception => e.printStackTrace()
    }
  }

  def deleteFileIfExists(filePath: String, isDir: Boolean = true): Unit = {
    if (filePath == null || filePath == "") {
      log.warn("filePath is null or ''")
    }
    try {
      if (exists(filePath)) {
        log.info(s"$filePath delete success and recursive is true")
        hdfs.delete(new Path(filePath), isDir)
      }
    } catch {
      case i: IllegalArgumentException => new IllegalArgumentException("hdfs未初始化，或者初始化失败")
      case e: Exception => e.printStackTrace()
    }
  }


  /**
    * 本地文件copy到hdfs上面
    *
    * @param localPath
    * @param destination
    * @return
    */
  def copyFromLocal(localPath: String, destination: String): Boolean = {
    try {
      if (hdfs == null) {
        log.warn("hdfs未初始化")
        false
      } else {
        hdfs.copyFromLocalFile(new Path(localPath), new Path(destination))
        true
      }
    } catch {
      case i: IllegalArgumentException =>
        new IllegalArgumentException("hdfs未初始化，或者初始化失败")
        false
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }

  /**
    * 读取hdfsjson文件
    *
    * @param filePath 文件路径
    * @return 返回map
    */
  def readHdfs2Json(filePath: String): util.Map[String, Object] = {
    var open: FSDataInputStream = null
    try {
      open = hdfs.open(new Path(filePath))
      val parse: util.Map[String, Object] = JSON.parse(open).asInstanceOf[java.util.Map[String, Object]]
      parse
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    } finally {
      open.close()
    }
  }

  /**
    * 一行一行读取文件，然后按照，切割文件
    * 第一个放入key第二个放入value
    *
    * @param filePath
    * @return
    */
  def readHdfs2Map(filePath: String): mutable.HashMap[String, String] = {
    var open: FSDataInputStream = null
    var inputStreamReader: InputStreamReader = null
    var reader: BufferedReader = null
    try {
      if (!hdfs.exists(new Path(filePath))) {
        log.warn(s"$filePath is not exists,return null")
        null
      } else {
        open = hdfs.open(new Path(filePath))
        inputStreamReader = new InputStreamReader(open)
        reader = new BufferedReader(inputStreamReader)
        var str = reader.readLine()
        val map = new mutable.HashMap[String, String]()
        while (str != null) {
          val split = str.split(",")
          map.put(split(0), split(1))
          str = reader.readLine()
        }
        map
      }
    } catch {
      case e: Exception =>
        log.error(e.printStackTrace())
        null
    } finally {
      reader.close()
      inputStreamReader.close()
      open.close()
    }
  }

  def readHdfs2Map(filePath: String, delimiter: String = ",", col1: Int = 0, col2: Int = 1): mutable.HashMap[String, String] = {
    var open: FSDataInputStream = null
    var inputStreamReader: InputStreamReader = null
    var reader: BufferedReader = null
    try {
      if (hdfs == null) {
        log.warn("hdfs未初始化")
      }
      if (!hdfs.exists(new Path(filePath))) {
        log.warn(s"$filePath is not exists,return null")
        null
      } else {
        open = hdfs.open(new Path(filePath))
        inputStreamReader = new InputStreamReader(open)
        reader = new BufferedReader(inputStreamReader)
        var str = reader.readLine()
        val map = new mutable.HashMap[String, String]()
        while (str != null) {
          val split = str.split(delimiter)
          map.put(split(col1), split(col2))
          str = reader.readLine()
        }
        map
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    } finally {
      reader.close()
      open.close()
    }
  }


  def readHdfstoInputMap(filePath: String, dicimit: String, map: mutable.HashMap[String, String]): mutable.HashMap[String, String] = {
    var open: FSDataInputStream = null
    var inputStreamReader: InputStreamReader = null
    var reader: BufferedReader = null
    try {
      if (hdfs == null) {
        log.warn("hdfs未初始化")
      }
      open = hdfs.open(new Path(filePath))
      inputStreamReader = new InputStreamReader(open)
      reader = new BufferedReader(inputStreamReader)
      var str = reader.readLine()
      while (str != null) {
        val split = str.split(dicimit)
        map.put(split(0).trim, split(1).trim)
        str = reader.readLine()
      }
      map
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    } finally {
      reader.close()
      inputStreamReader.close()
      open.close()
    }
  }

  def appendWrite(filePath: String, info: String): Unit = {
    var out: FSDataOutputStream = null
    var wrappedStream: OutputStream = null
    var writer: OutputStreamWriter = null
    var bufferedWriter: BufferedWriter = null
    try {
      if (hdfs.exists(new Path(filePath))) {
        out = hdfs.create(new Path(filePath), true)
      } else {
        out = hdfs.append(new Path(filePath))
      }
      wrappedStream = out.getWrappedStream
      writer = new OutputStreamWriter(wrappedStream)
      bufferedWriter = new BufferedWriter(writer)
      if (info == null || info.equals("")) {
        bufferedWriter.write(info)
      } else {
        bufferedWriter.write(info + "\n")
      }
      bufferedWriter.flush()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      bufferedWriter.close()
      wrappedStream.close()
      wrappedStream.close()
      out.close()
    }

  }


  /**
    * 获取正则表达式文件
    *
    * @param pathPattern
    * @return
    */
  def globStatus(pathPattern: String): Array[FileStatus] = hdfs.globStatus(new Path(pathPattern))


  def getPathPatternFile(pathPattern: String): Array[String] = globStatus(pathPattern).map(_.getPath.toString)


  def fileMerge(src: String, dst: String): Unit = {
    try {
      log.info(s"start merge $src to $dst")
      val srcPath = new Path(src)
      val dstPath = new Path(dst)
      deleteFileIfExists(dst, false)
      val config = new Configuration()
      FileUtil.copyMerge(hdfs, srcPath, hdfs, dstPath, false, configuration, null)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


  def rname(source: String, destination: String, overwrite: Boolean): Boolean = {
    if (StringUtils.isEmpty(source) || StringUtils.isEmpty(destination)) {
      log.error(s"rname is fail ; source:$source 、 destination:$destination")
      return false
    }
    if (exists(destination)) {
      if (overwrite) {
        deleteFile(destination, true)
        hdfs.rename(new Path(source), new Path(destination))
        true
      } else {
        throw new Exception("destination exists")
      }
    } else {
      hdfs.rename(new Path(source), new Path(destination))
      true
    }
  }

  def getOutout(filePath: String, overWriter: Boolean = true): FSDataOutputStream = {
    if (exists(filePath)) {
      if (overWriter) {
        deleteFile(filePath, true)
        hdfs.create(new Path(filePath))
      } else {
        hdfs.append(new Path(filePath))
      }
    } else {
      hdfs.create(new Path(filePath))
    }
  }

  def mkdirs(path: String) = {
    if (exists(path)) {
      log.warn(s"$path has already exists")
      false
    } else {
      hdfs.mkdirs(new Path(path))
    }
  }

  def createFile(path: String): Unit = {
    if (!exists(path)) {
      hdfs.createNewFile(new Path(path))
    }
  }

  def main(args: Array[String]): Unit = {
  }

}
