package com.victor.product

/**
  * Created by Administrator on 2018/11/12.
  * area,productid,count,citycounts
  */
case class Area_Top3_Product(
  area:String,
  productid:Int,
  count:Long,
  citycounts:String
)

case class Area_Top3_Product_Mysql(
                            taskid:String,
                            area:String,
                            productid:Int,
                            count:Long,
                            citycounts:String
                            )


