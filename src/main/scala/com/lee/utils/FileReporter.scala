package com.lee.utils

import java.io.PrintWriter

import com.lee.main.JobArgs
import org.apache.log4j.Logger

/**
  * Created by root on 2017/11/13.
  */
class FileReporter {
  private var writer: PrintWriter = null

  def init(): Unit = {
    var fileName = PathUtil.getLogPath + "/" + JobArgs.model + "_" + DateUtil.getCurMinStr() + ".log"
    writer = HDFSUtil.getWriter(fileName,true)
  }

  def close(): Unit = {
    writer.close()
  }

  def reportModelStcInfo(info: String) {
    FileReporter.log.info("\n"+info)
    writer.println(info+"\n")
    writer.flush()
  }


}
object FileReporter{
  val log = Logger.getLogger(getClass)
  private var _singlton = new FileReporter
  def singlton: FileReporter = _singlton
}