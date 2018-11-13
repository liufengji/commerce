/*
 * Copyright (c) 2017. Atguigu Inc. All Rights Reserved.
 * Date: 17-12-22 上午11:26.
 * Author: Administrator.
 */

package com.victor.advertise.mykafka_offset

import com.victor.common.ConfigManager
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

/**
  * 日志格式：
  * timestamp province city userid adid
  * 某个时间点 某个省份 某个城市 某个用户 某个广告
  */
object AdClickRealTimeStat_Offset {


  def main(args: Array[String]): Unit = {

    // 构建Spark上下文
    val sparkConf = new SparkConf().setAppName("streamingRecommendingSystem").setMaster("local[*]")

    // 创建Spark客户端
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // 设置检查点目录
    ssc.checkpoint("./streaming_checkpoint")

    // 获取Kafka配置
    val broker_list = ConfigManager.config.getString("kafka.broker.list")
    val topics = ConfigManager.config.getString("kafka.topics")
    val zookeeper = ConfigManager.config.getString("zookeeper.list")

    // kafka消费者配置
    val kafkaParam = Map[String,String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker_list,//用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "commerce-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    // 声明Kafka保存在Zookeeper上的路径对象
    val topicDirs = new ZKGroupTopicDirs("commerce-consumer-group",topics)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    // 创建一个Zookeeper的连接
    val zkClient = new ZkClient(zookeeper)

    // 获取偏移的保存目录下的子节点
    val children = zkClient.countChildren(zkTopicPath)

    // 生成的Kafka连接实例
    var adRealTimeLogDStream :InputDStream[(String,String)] = null

    // 如果子节点存在，则说明有偏移值保存
    if(children > 0){

      // 开始消费的偏移量
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      //---首先获取每一个Partition的主节点信息----
      val topicList = List(topics)
      val request = new TopicMetadataRequest(topicList,0)  //得到该topic的一些信息，比如broker,partition分布情况
      val getLeaderConsumer = new SimpleConsumer("linux",9092,10000,10000,"OffsetLookup") // low level api interface
      val response = getLeaderConsumer.send(request)  //TopicMetadataRequest   topic broker partition 的一些信息
      val topicMetaOption = response.topicsMetadata.headOption
      val partitions = topicMetaOption match{
        case Some(tm) =>
          tm.partitionsMetadata.map(pm=>(pm.partitionId,pm.leader.get.host)).toMap[Int,String]
        case None =>
          Map[Int,String]()
      }
      getLeaderConsumer.close()
      println("partitions information is: "+ partitions)
      println("children information is: "+ children)


      for (i <- 0 until children) {

        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        println(s"Partition【${i}】 目前的偏移值是:${partitionOffset}")

        val tp = TopicAndPartition(topics, i)
        //---获取当前Partition的最小偏移值【主要防止Kafka中的数据过期】-----
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))  // -2,1
        val consumerMin = new SimpleConsumer(partitions(i),9092,10000,10000,"getMinOffset")

        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets

        consumerMin.close()
        var nextOffset = partitionOffset.toLong
        if(curOffsets.length >0 && nextOffset < curOffsets.head){  //如果下一个offset小于当前的offset
          nextOffset = curOffsets.head
        }
        //---additional end-----
        println(s"Partition【${i}】 修正后的偏移值是:${nextOffset}")

        fromOffsets += (tp -> nextOffset)
      }
      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      println("从Zookeeper获取偏移量创建DStream")
      zkClient.close()
      adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)

    }else{
      println("直接创建DStream")
      adRealTimeLogDStream=KafkaUtils.createDirectStream[String,String,StringDecoder, StringDecoder](ssc,kafkaParam,Set(topics))
    }
    // 创建DStream，返回接收到的输入数据

    var offsetRanges = Array[OffsetRange]()

    // 需要没有转换之前获取偏移量
    val abc = adRealTimeLogDStream.transform{ rdd=>
      // 获取接收到的信息的当前偏移量
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    abc.map(_._2).foreachRDD{ rdd =>
      // 处理业务数据
      rdd.foreachPartition{ items =>

      }

      // 更新Zookeeper中的偏移值
      val updateTopicDirs = new ZKGroupTopicDirs("commerce-consumer-group",topics)
      val updateZkClient = new ZkClient(zookeeper)
      for(offset <- offsetRanges ){
        println(offset)
        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient,zkPath,offset.fromOffset.toString)
      }
      updateZkClient.close()

    }

    ssc.start()
    ssc.awaitTermination()
  }

}
