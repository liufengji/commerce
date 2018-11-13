package com.victor.advertise

import com.victor.common.ConfigManager
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


object AdvertiseAnysis {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("advertise").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    ssc.checkpoint("./checkpoint")

    val topic = ConfigManager.config.getString("kafka.topic")

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfigManager.config.getString("kafka.broker"), //用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "advertise",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    //TODO 需求一：广告统计黑名单
    // spark streaming 消费kafka数据
    val adRealtimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))

    //kafka 过滤掉黑名单的用户每天点击广告种类的数量数据
    val filtedAdRealTimeDStream = adRealtimeLogDStream.transform{ rdd =>

      //获取黑名单
      val blackList = AdBlackListDAO.findAll()

      rdd.map{ item =>
        val params = item._2.split(" ")
        AdvertiseRealTime(params(0).trim.toLong,params(1).trim.toInt,params(2).trim.toInt,params(3).trim.toInt,params(4).trim.toInt)
      }.filter(item => !blackList.contains(item.userid))
    }

    //用户这一天 5秒点击某一类广告的总数
    val dailyUserClickDStream = filtedAdRealTimeDStream.map{adRealtime =>
      ((DateFormatUtils.format(adRealtime.timestamp,"yyyy-MM-dd"),adRealtime.userid,adRealtime.adid),1L)
    }.reduceByKey(_ + _)

    dailyUserClickDStream.foreachRDD{rdd =>
      rdd.foreachPartition{ items =>
        val adUserClickCountArray = ArrayBuffer[AdUserClickCount]()
        for(item <- items){
          adUserClickCountArray += AdUserClickCount(item._1._1,item._1._2,item._1._3,item._2)
        }
        //修改用户这一天点击某一类广告的数量
        AdUserClickCountDAO.updateBatch(adUserClickCountArray.toArray)
      }
    }

    val blacklistDStream = dailyUserClickDStream.filter{ case ((date,userid,adid),count) =>
      //查询即将成为黑明单的用户，这一天点击某类广告的数量大于100
      if(AdUserClickCountDAO.findClickCountByMultiKey(date,userid.toLong,adid.toLong) >=100)
        true
      else
        false
    }

    // 保存黑名单
    blacklistDStream.foreachRDD{rdd =>
      rdd.foreachPartition{ items =>
        val adBalckList = ArrayBuffer[AdBlacklist]()
        for(item <- items){
          adBalckList += AdBlacklist(item._1._2)
        }
        AdBlackListDAO.insertBatch(adBalckList.toArray)
      }
    }

    // TODO 需求二：广告点击实时统计
    // TODO 每天各省各城市各广告的点击流量实时统计
    val dailyAdClickDStream = filtedAdRealTimeDStream.map{advertiserealtime =>
      ((DateFormatUtils.format(advertiserealtime.timestamp,"yyyy-MM-dd"),advertiserealtime.province,advertiserealtime.city,advertiserealtime.adid),1L)
    }.reduceByKey(_+_)

    val aggregatedDStream = dailyAdClickDStream.updateStateByKey[Long]{(values:Seq[Long],old:Option[Long]) =>
      Some(old.getOrElse[Long](0)+values.sum)
    }

    aggregatedDStream.foreachRDD{rdd =>

      rdd.foreachPartition{items =>

        val adStats = ArrayBuffer[AdStat]()
        for(item <- items){
          adStats += AdStat(item._1._1,item._1._2,item._1._3,item._1._4,item._2)
        }
        AdStatDAO.updateBatch(adStats.toArray)
      }
    }

    // TODO 需求三：实时统计各省最热广告(各省点击Top3)

    val provinceTop3DStream = aggregatedDStream.transform{ rdd =>

      // 每天每省每个广告的实时点击流量
      val aggrDateAdCount = rdd.map{ case ((date,province,city,adid),count) =>
        ((date,province,adid),count)
      }.reduceByKey(_ + _)

      import spark.implicits._
      val aggrDateAdCountDF = aggrDateAdCount.map{ case ((date,province,adid),count) =>
        (date,province,adid,count)
      }.toDF("date","province","adid","count")

      aggrDateAdCountDF.createOrReplaceTempView("aggrDateAdCountDF")

      val provinceTop3DF = spark.sql("select date,province, adid, count from " +
        "(select date, province, adid, count, rank() over(partition by date,province order by count desc) rank from aggrDateAdCountDF) t " +
        "where rank <=3")

      provinceTop3DF.rdd
    }

    provinceTop3DStream.foreachRDD{rdd =>

      rdd.foreachPartition{items =>

        val adProvinceTop3Array = ArrayBuffer[AdProvinceTop3]()

        for(item <- items){

          val date = item.getAs[String]("date")
          val province = item.getAs[Int]("province")
          val adid = item.getAs[Int]("adid")
          val count = item.getAs[Long]("count")
          adProvinceTop3Array += AdProvinceTop3(date,province,adid,count)
        }
        AdProvinceTop3DAO.updateBatch(adProvinceTop3Array.toArray)
      }

    }

    // TODO 需求四： 最近一小时广告点击趋势统计
    val minute2Count = filtedAdRealTimeDStream.transform{rdd =>
      rdd.map{ item =>
        ((DateFormatUtils.format(item.timestamp,"yyyy-MM-dd HH:mm"),item.adid),1L)
      }
    }

    //reduceByKeyAndWindow 窗口函数 窗口大小 60分钟，窗口移动 30秒
    val windowDstream = minute2Count.reduceByKeyAndWindow((a:Long,b:Long) => a + b, Minutes(60),Seconds(30))

    // 执行周期  1分钟执行
    windowDstream.foreachRDD{rdd =>

      rdd.foreachPartition{items =>
        val adClickTrendArray = ArrayBuffer[AdClickTrend]()
        for (item <- items){
          val date = item._1._1.split(" ")(0)
          val hour = item._1._1.split(" ")(1).split(":")(0)
          val minute = item._1._1.split(" ")(1).split(":")(1)

          val adid = item._1._2
          val count = item._2
          adClickTrendArray += AdClickTrend(date,hour,minute,adid,count)
        }
        AdClickTrendDAO.updateBatch(adClickTrendArray.toArray)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}