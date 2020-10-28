package com.wqj.flink1.utils

import java.io.File

import com.wqj.flink1.base.OperateHbase


object FileUtil {

  /**
    * 返回配置文件的路径
    * */
  def getHadoopConf(): String = {
    var rootPath = OperateHbase.getClass.getResource("").getPath.replaceAll("classes\\S{1,100}", "classes/")
    if(rootPath.startsWith("/")) rootPath=rootPath.replaceFirst("/","")
    val filePth = "/etc/hadoop/conf"
    val file = new File(filePth)
    if (!file.exists) { // 文件不存在
      return rootPath
    }
    return filePth
  }
}
