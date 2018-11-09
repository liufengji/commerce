package com.victor.common

import java.util.Calendar

import org.apache.commons.lang3.time.DateFormatUtils

/**
  * Created by Administrator on 2018/11/7.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val currentDate = Calendar.getInstance().getTime
    val date = DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd")
    val sysdate = System.currentTimeMillis()
    println(currentDate)
    println(date)
    println(sysdate)

    val starttime = "2018-11-07 13:51:58"
    val endtime = "2018-11-08 13:51:58"

    val boolean = Utils.before(starttime,endtime)
    println(boolean)
  }
}
