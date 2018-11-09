package com.victor.common

import org.joda.time.format.DateTimeFormat

object Utils {

  //线程安全的，专门处理时间问题
  val DATATIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def fullFill(time: String): String = {
    if (time.length == 1)
      "0" + time
    else
      time
  }

  def before(starttime:String, endtime:String):Boolean ={
    DATATIME_FORMAT.parseDateTime(starttime).isBefore(DATATIME_FORMAT.parseDateTime(endtime))
  }

  def after(starttime:String, endtime:String):Boolean ={
    before(endtime,starttime)
  }

  // 返回秒
  def getDateDuration(endtime:String, starttime:String):Long={
    (DATATIME_FORMAT.parseDateTime(endtime).getMillis - DATATIME_FORMAT.parseDateTime(starttime).getMillis) / 1000
  }

}

/**
  * 数字格工具类
  *
  * @author wuyufei
  *
  */
object NumberUtils {

  /**
    * 格式化小数
    * @param scale 四舍五入的位数
    * @return 格式化小数
    */
  def formatDouble(num:Double, scale:Int):Double = {
    val bd = BigDecimal(num)
    bd.setScale(scale, BigDecimal.RoundingMode.HALF_UP).doubleValue()
  }

}


