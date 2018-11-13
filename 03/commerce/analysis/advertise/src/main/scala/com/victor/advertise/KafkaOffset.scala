package com.victor.advertise

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
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaOffset {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("advertise").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = ConfigManager.config.getString("kafka.topic")
    val zooKeeper = ConfigManager.config.getString("zookeeper")

    //Zookeeper DIR
    val topicDirs = new ZKGroupTopicDirs("advertise", topic)
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    val zkClient = new ZkClient(zooKeeper)

    val children = zkClient.countChildren(zkTopicPath)

    var adRealtimeLogDStream: InputDStream[(String, String)] = null

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> ConfigManager.config.getString("kafka.broker"), //用于初始化链接到集群的地址
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      //用于标识这个消费者属于哪个消费团体
      ConsumerConfig.GROUP_ID_CONFIG -> "advertise",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    if (children > 0) {
     println("从Zookeeper恢复")
     var fromOffsets: Map[TopicAndPartition, Long] = Map()

     // 为了获得一个topic 所有的Pattiton的主分区Hostname
     val topicList = List(topic)
     val request = new TopicMetadataRequest(topicList, 0)
     val getLeaderConsumer = new SimpleConsumer("hadoop102", 9092, 10000, 10000, "OffsetLookup")
     val response = getLeaderConsumer.send(request)

      //topic 元数据 信息
     val topicMetadataOption = response.topicsMetadata.headOption
     val partitons = topicMetadataOption match {
       case Some(tm) =>
         tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
       case None => Map[Int, String]()
     }

     getLeaderConsumer.close()

     println("partitions info is:" + partitons)
     println("children info is:" + children)

     for (i <- 0 until children) {

       val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
       println("保存的偏移位置是：" + partitionOffset)
       val tp = TopicAndPartition(topic, i)

       val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
       val consumerMin = new SimpleConsumer(partitons(i), 9092, 10000, 10000, "getMinOffset")
       val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets

       consumerMin.close()

       var nextOffset = partitionOffset.toLong
       if (curOffsets.length > 0 && nextOffset < curOffsets.head) {
         nextOffset = curOffsets.head
       }
       println("修正后的偏移位置是：" + nextOffset)
       fromOffsets += (tp -> nextOffset)
     }

     val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
     adRealtimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)

   } else {
     println("直接创建")
     adRealtimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
   }

    //adRealtimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))

    var offsetRanges = Array[OffsetRange]()

    val adRealtimeLog = adRealtimeLogDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    adRealtimeLog.foreachRDD { rdd =>

      //OffsetRange(topic: 'advertise', partition: 0, range: [6222 -> 6324])
      for (offset <- offsetRanges)
        println(offset)

      rdd.foreachPartition { items =>
        // 处理了业务
        for (item <- items) {
          //println(item)
        }
      }

      // 创建一个Zookeeper的目录
      val updateTopicDir = new ZKGroupTopicDirs("advertise", topic)

      // 创建一个Zookeeper的客户端
      val updateZkClient = new ZkClient(zooKeeper)

      // 保存了所有Partition的信息
      for (offset <- offsetRanges) {
        val zkPath = s"${updateTopicDir.consumerOffsetDir}/${offset.partition}"
        //OffsetRange(topic: 'advertise', partition: 0, range: [4590 -> 4692])
        //ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.untilOffset.toString)
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
      }

      updateZkClient.close()

    }
    ssc.start()
    ssc.awaitTermination()

  }

}
