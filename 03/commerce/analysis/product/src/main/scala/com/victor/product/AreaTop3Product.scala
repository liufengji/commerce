package com.victor.product

import java.util.UUID

import com.victor.common.{ConfigManager, Constants, ProductInfo}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaTop3Product {

  def main(args: Array[String]): Unit = {

    val taskid = UUID.randomUUID().toString

    //获取任务的配置信息
    val task_json = ConfigManager.config.getString("task.params.json")
    val task = JSONObject.fromObject(task_json)

    val startDate = task.getString("startDate")
    val endDate = task.getString("endDate")

    val sparkConf = new SparkConf().setAppName("product").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //从Hive中获取用户行为数据，  根据日期和是否click
    import spark.implicits._
    val userVisitActionRDD = spark.sql("select city_id, click_product_id from "+Constants.TABLE_USER_VISIT_ACTION + " where " +
      "date >='"+startDate+"' and date <='"+endDate +"' and click_product_id != '' ").rdd


    //将改数据转换为 cityid_productid,1L
    val cityProduct2Counts = userVisitActionRDD.map{row =>
      (row.getAs[String]("city_id") + "_" + row.getAs[String]("click_product_id"), 1L)
    }.reduceByKey(_+_)

    val productInfoRDD = spark.sql("select * from "+Constants.TABLE_PRODUCT_INFO).as[ProductInfo].rdd

    val productId2productInfoRDD = productInfoRDD.map{item => (item.product_id,item)}

    //reduceByKey聚合， 拆分（cityid，productid，count）  并JOIN 产品数据
    val cityid2ProductInfo = cityProduct2Counts.map{item =>
      val city_id = item._1.split("_")(0)
      val product_id = item._1.split("_")(1)
      (product_id.toInt,(city_id.toInt,item._2))
    }.join(productId2productInfoRDD).map{case (productid,((cityid,count),productInfo)) =>

      (cityid,(productid,count,productInfo.product_name,productInfo.extend_info))
    }

    //将区域数据和该数据进行JOIN，连接  cityname + count
    val areaRDD = sc.makeRDD(Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北")))

    val cityid2AreaRDD = areaRDD.map(item => (item._1.toInt,item))

    //import org.apache.spark.sql.functions.con

    spark.udf.register("concat_city", new GroupConcatCitycounts)

    val aggrProductInfoDF = cityid2ProductInfo.join(cityid2AreaRDD).map{ case (cityid,((productid,count,name,extend),(city,cityname,area)))=>
      (area,count,cityname+":"+count,productid,name,extend)
    }.toDF("area","count","citycount","productid","productname","productextend")

    aggrProductInfoDF.createOrReplaceTempView("aggrProductInfo")

    //("area","cityid","cityname + ":" + click", productid)
    //groupby area，productid

    val areaAggrDF = spark.sql("select area, productid,sum(count) count , concat_city(citycount) citycounts from aggrProductInfo group by area,productid")

    areaAggrDF.createOrReplaceTempView("areaAggrDF")

    //开窗函数
    val area_top3_productRDD = spark.sql("select area,productid,count,citycounts from (select area," +
      "productid," +
      "count," +
      "citycounts," +
      "rank() over(partition by area order by count desc) rank from areaAggrDF) t" +
      " where rank <=3").as[Area_Top3_Product].rdd

    //area_top3_productDF.show(100)
    //area_top3_productDF.as[Area_Top3_Product].rdd
    val area_top3_product_mysqlRDD = area_top3_productRDD.map{ item =>
      Area_Top3_Product_Mysql(UUID.randomUUID().toString,item.area,item.productid,item.count,item.citycounts)
    }


    //结果写入MySQL
    area_top3_product_mysqlRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigManager.config.getString("jdbc.url"))
      .option("dbtable", "area_top3_product_mysql")
      .option("user", ConfigManager.config.getString("jdbc.username"))
      .option("password", ConfigManager.config.getString("jdbc.password"))
      .mode(SaveMode.Append)
      .save()

    spark.stop()

  }

}
