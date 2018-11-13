package com.victor.session

import java.util.UUID

import com.victor.common._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//用户行为分析
object SessionAnalysis {

  def main(args: Array[String]): Unit = {

    //TODO 需求一 session 聚合占比
    val taskid = UUID.randomUUID().toString

    //获取任务的配置信息
    val task_json = ConfigManager.config.getString("task.params.json")
    val task = JSONObject.fromObject(task_json)

    val startDate = task.getString("startDate")
    val endDate = task.getString("endDate")
    val startAge = task.getString("startAge")
    val endAge = task.getString("endAge")
    val professionals = task.getString("professionals")
    val cities = task.getString("cities")
    val sex = task.getString("sex")
    val keywords = task.getString("keywords")
    val categoryIds = task.getString("categoryIds")

    //创建sparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //根据配置信息从Hive中获取用户行为数据
    import spark.implicits._
    val userVisitActionDF = spark.sql("select * from " + Constants.TABLE_USER_VISIT_ACTION +
      " where date >= ' " + startDate + " and date <=" + endDate +"'").as[UserVisitAction]

    val userVisitActionRDD = userVisitActionDF.rdd

    //将行为数据转换成K-V结构，sessionid
    val sessionid2userVisitActionRDD = userVisitActionRDD.map(item => (item.session_id, item))

    //将Sessionid相同的数据进行聚合，计算出访问步长、访问时长, 将K转变为 userid
    val sessionid2userVisitActionsRDD = sessionid2userVisitActionRDD.groupByKey()

    //计算访问步长，访问时长
    val userid2partAggrInfo = sessionid2userVisitActionsRDD.map { case (sessionid, userVisitActions) =>

      val search_keywords = new mutable.HashSet[String]()
      val clickCategoryIDs = new mutable.HashSet[String]()

      var starttime = ""
      var endtime = ""
      var userid = -1

      //访问步长
      var step_visit = 0

      userVisitActions.foreach { userVisitAction =>
        if (userid == -1)
          userid = userVisitAction.user_id

        step_visit += 1

        //计算开始时间和结束时间
        if (starttime == "") {
          starttime = userVisitAction.action_time
        }

        if (endtime == "") {
          endtime = userVisitAction.action_time
        }

        //endtime 在 action_time 之前， 返回true
        if (Utils.before(endtime, userVisitAction.action_time)) {
          endtime = userVisitAction.action_time
        }

        if (Utils.before(userVisitAction.action_time, starttime)) {
          starttime = userVisitAction.action_time
        }

        //将search——keyword添加到set
        if (userVisitAction.search_keyword != "") {
          search_keywords += userVisitAction.search_keyword
        }

        if (userVisitAction.click_category_id != "") {
          clickCategoryIDs += userVisitAction.click_category_id
        }
      }
      //访问时长
      val visit_time = Utils.getDateDuration(endtime, starttime)
      (userid, PartAggrInfo(sessionid, search_keywords, clickCategoryIDs, visit_time, step_visit, starttime))
    }

    //从Hive中获取用户数据
    val userInfoDS = spark.sql("select * from "+Constants.TABLE_USER_INFO).as[UserInfo]
    val userInfoRDD = userInfoDS.rdd

    val userid2userInfoRDD = userInfoRDD.map(item => (item.user_id, item))

    //将用户数据和聚合后的Session进行 JOIN，生成新的聚合数据
    val sessionid2fullAggrInfo = userid2partAggrInfo.join(userid2userInfoRDD).map{ case (userid, (partAggrInfo, userInfo)) =>
      (partAggrInfo.session_id, FullAggrInfo(partAggrInfo,userInfo))
    }

    //累加器
    val sessionAggrStatAccumulator = new SessionAggrStatAccumulator
    sc.register(sessionAggrStatAccumulator,"sessionAggrStatAccumulator")

    //将数据根据任务的配置信息进行过滤，在过滤的过程中更新累加器
    val filteredSessionid2FullAggrInfo = sessionid2fullAggrInfo.filter{ case (sessionid,fullAggrInfo) =>

      var success = true
      //检测条件，如果有条件不满足，将success设置为false

      if(success){

        //更新累加器
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)

        // 计算访问时长范围
        def calculateVisitLength(visitLength: Long) {
          if (visitLength >= 1 && visitLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
          } else if (visitLength >= 4 && visitLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
          } else if (visitLength >= 7 && visitLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
          } else if (visitLength >= 10 && visitLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
          } else if (visitLength > 30 && visitLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
          } else if (visitLength > 60 && visitLength <= 180) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
          } else if (visitLength > 180 && visitLength <= 600) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
          } else if (visitLength > 600 && visitLength <= 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
          } else if (visitLength > 1800) {
            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
          }
        }

        // 计算访问步长范围
        def calculateStepLength(stepLength: Long) {
          if (stepLength >= 1 && stepLength <= 3) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
          } else if (stepLength >= 4 && stepLength <= 6) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
          } else if (stepLength >= 7 && stepLength <= 9) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
          } else if (stepLength >= 10 && stepLength <= 30) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
          } else if (stepLength > 30 && stepLength <= 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
          } else if (stepLength > 60) {
            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
          }
        }

        calculateVisitLength(fullAggrInfo.partAggrInfo.visit_time)
        calculateStepLength(fullAggrInfo.partAggrInfo.step_length)
        //sessionAggrStatAccumulator.add()
      }

      success
    }

    filteredSessionid2FullAggrInfo.count()

    filteredSessionid2FullAggrInfo.cache()

    //计算累加器的值
    val value = sessionAggrStatAccumulator.value

    // 从Accumulator统计串中获取值
    val session_count = value(Constants.SESSION_COUNT).toDouble

    //时长
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    //步长
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 计算各个访问时长和访问步长的占比,保存两位小数后四舍五入
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = SessionAggrStat(taskid,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    //写入MySQL数据库
    import spark.implicits._
    val sessionAggrStatRDD = spark.sparkContext.makeRDD(Array(sessionAggrStat))
    sessionAggrStatRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigManager.config.getString("jdbc.url"))
      .option("dbtable", "session_aggr_stat")
      .option("user", ConfigManager.config.getString("jdbc.username"))
      .option("password", ConfigManager.config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .save()


    //TODO  需求二：随机抽取Session

    //1、将过滤后的数据和原始的session数据集进行join,过滤掉不合要求的数据
    val filtedUserVisitActionRDD = filteredSessionid2FullAggrInfo.join(sessionid2userVisitActionRDD).map{case (sessionid,(fullaggrinfo,uservisitaction)) =>
      (sessionid,uservisitaction)
    }
    //2、将filtersession 修改为 yyyy-MM-dd_HH粒度
    val dateHour2FullAggrInfo = filteredSessionid2FullAggrInfo.map{case(sessionid,fullaggrinfo) =>
      //yyyy-MM-dd HH:mm:ss
      val starttime = fullaggrinfo.partAggrInfo.session_time.split(":")(0)
      (starttime,fullaggrinfo)
    }

    //3、计算每个小时session的数据
    val dateHourCountMap = dateHour2FullAggrInfo.countByKey

    //4、将结果转换成  天 + 小时 粒度
    val date2HourCountMap = mutable.HashMap[String,mutable.HashMap[String,Int]]()

    for((dateHour,count) <- dateHourCountMap){
      val date = dateHour.split(" ")(0)
      val hour = dateHour.split(" ")(1)

      date2HourCountMap.get(date) match{
        case None => date2HourCountMap(date) = new mutable.HashMap[String,Int]();date2HourCountMap(date) += (hour -> count.toInt)
        case Some(hourCountMap) => hourCountMap += (hour -> count.toInt)
      }
    }

    //5、根据每小时的比例，生成堆积的index序列

    //总共抽取session的数量
    val siphonSessionCount = 100

    //获取每天需要抽取session的数量
    val extractDayNumber = siphonSessionCount / date2HourCountMap.size

    //最终抽取的index下标数
    val extractDate2HourIndex = mutable.HashMap[String,mutable.HashMap[String,mutable.ListBuffer[Int]]]()

    /**
      * 根据每个小时应该抽取的数量，来产生随机值
      * @param hourIndexMap   主要用来存放生成的随机值
      * @param dateSessionCount   当天所有session总数
      * @param hourCountMap   每个小时的session总数
      */
    def hourExtractMapFunc(hourIndexMap:mutable.HashMap[String,mutable.ListBuffer[Int]],
                           dateSessionCount:Int,
                           hourCountMap:mutable.HashMap[String,Int]): Unit ={
      val random = new Random()

      for((hour,count) <- hourCountMap){

        //求出每个小时，应该抽取几个session
        var hourExtractNumber = ((count / dateSessionCount.toDouble) *  extractDayNumber).toInt
        if (hourExtractNumber > count)
          hourExtractNumber = count

        hourIndexMap.get(hour) match{
          case None =>  hourIndexMap(hour) = new mutable.ListBuffer[Int]();
            for (i <- 0 to hourExtractNumber){
              //Count是当前小时所有的数据个数
              var index = random.nextInt(count.toInt)
              while(hourIndexMap(hour).contains(index)){
               index = random.nextInt(count.toInt)
              }
              hourIndexMap(hour) += (index)
            }
          case Some(indexList) =>
            for (i <- 0 to hourExtractNumber){
              //Count是当前小时所有的数据个数
              var index = random.nextInt(count.toInt)
              while(hourIndexMap(hour).contains(index)){
                index = random.nextInt(count.toInt)
              }
              hourIndexMap(hour) += (index)
            }
        }
      }

    }
    for((date,hourCountMap) <- date2HourCountMap){
      //获取天级别 session的总数
      val dateSessionCount = hourCountMap.values.sum

      extractDate2HourIndex.get(date) match {
        case None => extractDate2HourIndex(date) = new mutable.HashMap[String,mutable.ListBuffer[Int]]();//更新index
          hourExtractMapFunc(extractDate2HourIndex(date),dateSessionCount,hourCountMap)
        case Some(hourIndexMap) => //更新index
          hourExtractMapFunc(extractDate2HourIndex(date),dateSessionCount,hourCountMap)
      }
    }

    //6、将整个Map做成广播变量
    val extractDate2HourIndexBroadCast = sc.broadcast(extractDate2HourIndex)

    //7、将第二步 yyyy-MM-dd_HH粒度的数据做 groupByKey操作
    val dataHour2FullAggrInfos = dateHour2FullAggrInfo.groupByKey()

    //8、将数据进行flatMap,根据广播变量，抽取相应的session
    val sessionRandomExtactRDD = dataHour2FullAggrInfos.flatMap{ case (dateHour,fullAggrInfos) =>


      val date = dateHour.split(" ")(0)
      val hour = dateHour.split(" ")(1)
      //extractDate2HourIndex
      val date2HourExtactorMap = extractDate2HourIndexBroadCast.value

      //当前小时需要的index集合
      val currentHourIndex = date2HourExtactorMap.get(date).get(hour)

      var index = 0

      //抽取相应session存放处
      val sessionRandomExtactArray = new ArrayBuffer[SessionRandomExtract]()

      for(fullAggrInfo <- fullAggrInfos){

        if(currentHourIndex.contains(index)){
          sessionRandomExtactArray += SessionRandomExtract(taskid,fullAggrInfo.partAggrInfo.session_id,
            fullAggrInfo.partAggrInfo.session_time,fullAggrInfo.partAggrInfo.search_keywords.mkString("|"),
            fullAggrInfo.partAggrInfo.click_category_ids.mkString("|"))
        }
        index += 1

      }
      sessionRandomExtactArray
    }

    //9、将抽取后的数据保存到mysql
    import spark.implicits._
    sessionRandomExtactRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigManager.config.getString("jdbc.url"))
      .option("dbtable", "session_random_extract")
      .option("user", ConfigManager.config.getString("jdbc.username"))
      .option("password", ConfigManager.config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .save()

    //10、将抽取后的数据，提取所有的sessionid
    val sessionid2ExtactSession = sessionRandomExtactRDD.map(item => (item.sessionid,item.sessionid))

    //11、将抽取的sessionid 和 过滤后的用户行为数据进行join,缩小整个数据结果集
    val sessionDetailRDD = sessionid2ExtactSession.join(filtedUserVisitActionRDD).map{case (sessionid,(sessid,userVisitAction)) =>

         SessionDetail(
           taskid,
           userVisitAction.user_id,
           sessionid,
           userVisitAction.page_id,
           userVisitAction.action_time,
           userVisitAction.search_keyword,
           userVisitAction.click_category_id,
           userVisitAction.click_product_id,
           userVisitAction.order_categroy_ids,
           userVisitAction.order_product_ids,
           userVisitAction.pay_categroy_ids,
           userVisitAction.pay_product_ids
         )

    }

    //12、将结果集妆花为class对象，保存到mysql数据库
    import spark.implicits._
    sessionDetailRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigManager.config.getString("jdbc.url"))
      .option("dbtable", "session_detail")
      .option("user", ConfigManager.config.getString("jdbc.username"))
      .option("password", ConfigManager.config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .save()


    //TODO 需求三：统计TOP10的品类

    //第一步：计算各个品类的点击数量、订单数量、付款次数
    //将数据转换成为 品类_动作 粒度   order_product_ids=1,2,3,45 flatmap
    //filtedUserVisitActionRDD 是复合条件的session数据  (sessionid,uservisitaction)
    val categoryAction2countRDD = filtedUserVisitActionRDD.map{ case (sessionid, userVisitAction) =>

      var categoryAction = ("buok",1L)

      if(userVisitAction.click_category_id != ""){
        categoryAction = (userVisitAction.click_category_id+"_click",1L)
      }else if(userVisitAction.pay_categroy_ids != ""){
        categoryAction = (userVisitAction.pay_categroy_ids+"_pay",1L)
      }else if(userVisitAction.order_categroy_ids != ""){
        categoryAction = (userVisitAction.order_categroy_ids+"_order",1L)
      }
      categoryAction
    }

    //聚合
    val reducedCategoryAction2countRDD = categoryAction2countRDD.filter(_._1 != "buok").reduceByKey(_+_)

    //将粒度转换为品类级别
    val categroy2ActionCountRDD = reducedCategoryAction2countRDD.map{ case (categroyAction,count) =>
      val categroy = categroyAction.split("_")(0)
      val action = categroyAction.split("_")(1)
      (categroy,(action,count))
    }

    //groupbykey
    val groupCategroy2ActionCount = categroy2ActionCountRDD.groupByKey()

    //map 成 可排序的class
    val categorySortKey2categroyidRDD = groupCategroy2ActionCount.map{ case (categroyid, actions) =>

      var clickCount = 0
      var orderCount = 0
      var payCount = 0

      for(action <- actions){
        if(action._1 == "click")
          clickCount += action._2.toInt
        if(action._1 == "order")
          orderCount += action._2.toInt
        if(action._1 == "pay")
          payCount += action._2.toInt
      }
      (CategorySortKey(clickCount,orderCount,payCount),categroyid)
    }

    //sortBykey  take 10
    //true 是升序  false 是降序
    val top10Array = categorySortKey2categroyidRDD.sortByKey(false).take(10)

    //保存到MySQL
    val top10CategoryArray = top10Array.map{case (categroySortKey,categroyid) =>
      Top10Category(UUID.randomUUID ().toString,categroyid, categroySortKey.clickCount,categroySortKey.orderCount,categroySortKey.payCount)
    }

    import spark.implicits._
    val top10DF = spark.sparkContext.makeRDD(top10CategoryArray).toDF()

    top10DF.write
      .format("jdbc")
      .option("url", ConfigManager.config.getString("jdbc.url"))
      .option("dbtable", "top10_category")
      .option("user", ConfigManager.config.getString("jdbc.username"))
      .option("password", ConfigManager.config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .save()


    //TODO 需求四：计算活跃Session
    //val categroyidArray = top10CategoryArray.map(_.categoryid).toArray
    val categroyidArray = top10CategoryArray.map(_.categoryid)

    //广播
    val categroyidArrayBrocast = sc.broadcast(categroyidArray)

    // MAP side JOIN
    val filtedActionRDD = filtedUserVisitActionRDD.filter{ case (sessionid,userVisitAction) =>
      var success = false

      if(userVisitAction.click_category_id != ""){

        val categroyidArrayBrocastArray = categroyidArrayBrocast.value

        if(categroyidArrayBrocastArray.contains(userVisitAction.click_category_id))
          success = true

      }
      success
    }

    val sessionidCategroyid2Count = filtedActionRDD.map{ case (sessionid,userVisitAction) =>
      (sessionid+"_"+userVisitAction.click_category_id, 1L)
    }

    val reducedCount = sessionidCategroyid2Count.reduceByKey(_+_)

    val groupCategroyid2Array = reducedCount.map{ case (sessionAndCategroyid,count) =>
      val sessionid = sessionAndCategroyid.split("_")(0)
      val categroyid = sessionAndCategroyid.split("_")(1)
      (categroyid,(sessionid,count))
    }.groupByKey()


    val sss = groupCategroyid2Array.flatMap{ case (categroyid,items) =>

      items.toArray.sortBy(_._2).reverse.take(10).map{ case (sessionid,count) =>
        Top10Session(taskid,categroyid,sessionid,count)
      }
      //排序的第二种方法
      //items.toArray.sortWith{}
    }

    import spark.implicits._
    sss.toDF.write
      .format("jdbc")
      .option("url", ConfigManager.config.getString("jdbc.url"))
      .option("dbtable", "top10_session")
      .option("user", ConfigManager.config.getString("jdbc.username"))
      .option("password", ConfigManager.config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .save()


    //关闭Spark
    spark.close()
  }

}
