package com.victor.page


import java.util.UUID

import com.victor.common._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object PageConvertRate {

  def main(args: Array[String]): Unit = {

    val taskid = UUID.randomUUID().toString

    //获取任务的配置信息
    val task_json = ConfigManager.config.getString("task.params.json")
    val task = JSONObject.fromObject(task_json)

    val startDate = task.getString("startDate")
    val endDate = task.getString("endDate")
    val tagetPageFlow = task.getString("targetPageFlow").split(",").toList
    //拉链
    val tagetPagePairs = tagetPageFlow.slice(0,tagetPageFlow.length-1).zip(tagetPageFlow.tail).map(item => item._1 +"_"+ item._2)

    val sparkConf = new SparkConf().setAppName("ConvertRate").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //广播变量
    val tagetPagePairsBrocast = spark.sparkContext.broadcast(tagetPagePairs)

    //TODO 需求五 计算页面跳转率
    //第一步：加载用户行为数据
    import spark.implicits._
    val userVisitActionRDD = spark.sql("select * from "+Constants.TABLE_USER_VISIT_ACTION+" " +
      "where date >='"+startDate+"' and date <='"+ endDate +"'").as[UserVisitAction].rdd

    //第二步：将用户行为数据转换为  Sessionid 2 UserVisitAction
    val sessionid2userVisitAction = userVisitActionRDD.map(item => (item.session_id, item))

    //第三步：将相同Session的数据聚集
    val sessionid2userVisitActions = sessionid2userVisitAction.groupByKey()

    //第四步：根据相同Session内每个行为的时间排序。得出 pageFlow， 将pageFlow转换为 A_B 结构，并根据targetFlow 过滤。
    val pageSplitRDD = sessionid2userVisitActions.flatMap{ case (sessionid, userVisitActions) =>

      val sortedUserVisitActions = userVisitActions.toList.sortWith((userv1,userv2) => Utils.after(userv2.action_time, userv1.action_time))
      val pageFlow = sortedUserVisitActions.map(_.page_id)
      //      scala> val list1 = List("1","2","3")
      //      list1: List[String] = List(1, 2, 3)
      //
      //      scala> val list2 = List("a","b")
      //      list2: List[String] = List(a, b)
      //
      //      scala> println(list1 zip list2)
      //      List((1,a), (2,b))
      val sessionPagePairs = pageFlow.slice(0,pageFlow.length-1).zip(pageFlow.tail).map{case (page1,page2) =>
        page1+"_"+page2
      }
      val tagetPagePairsB = tagetPagePairsBrocast.value
      sessionPagePairs.filter(tagetPagePairsB.contains(_)).map((_,1))
    }

    //第五步：通过CountByKey操作将pageFlow计数
    val pageSplitPvMap = pageSplitRDD.countByKey()

    //第六步：计算首页的PV
    val startPageId = tagetPageFlow.head

    val startPagePV = sessionid2userVisitAction.filter(_._2.page_id == startPageId).count()

    //第七步：计算每个页面的单跳转化率。保存到数据库。
    val convertRateMap = scala.collection.mutable.HashMap[String,Double]()

    var lastPageSplitPv = startPagePV.toDouble

    for(tagePage <- tagetPagePairs){

      val targetPagePv = pageSplitPvMap.get(tagePage).get.toDouble

      val pageConvertRate = targetPagePv / lastPageSplitPv

      convertRateMap.put(tagePage, NumberUtils.formatDouble(pageConvertRate,2))

      lastPageSplitPv = targetPagePv
    }

    val convertRateArray = for( (k,v) <- convertRateMap) yield {
      ConvertRate(taskid,k,v)
    }

    val convertRateRDD = spark.sparkContext.makeRDD(convertRateArray.toArray)

    import spark.implicits._
    convertRateRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigManager.config.getString("jdbc.url"))
      .option("dbtable", "page_split_convert_rate")
      .option("user", ConfigManager.config.getString("jdbc.username"))
      .option("password", ConfigManager.config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .save()

    spark.stop()

  }

}

