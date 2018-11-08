package com.victor.session

import com.victor.common.UserInfo

import scala.collection.mutable

// 部分的聚合信息
case class PartAggrInfo(session_id:String,
                        search_keywords:mutable.HashSet[String],
                        click_category_ids:mutable.HashSet[String],
                        visit_time:Long,
                        step_length:Int,
                        session_time:String)

// 全部的聚集信息
case class FullAggrInfo(partAggrInfo:PartAggrInfo,
                        userInfo:UserInfo)

/**
  * 聚合统计表
  *
  * @param taskid                       当前计算批次的ID
  * @param session_count                所有Session的总和
  * @param visit_length_1s_3s_ratio     1-3sSession访问时长占比
  * @param visit_length_4s_6s_ratio     4-6sSession访问时长占比
  * @param visit_length_7s_9s_ratio     7-9sSession访问时长占比
  * @param visit_length_10s_30s_ratio   10-30sSession访问时长占比
  * @param visit_length_30s_60s_ratio   30-60sSession访问时长占比
  * @param visit_length_1m_3m_ratio     1-3mSession访问时长占比
  * @param visit_length_3m_10m_ratio    3-10mSession访问时长占比
  * @param visit_length_10m_30m_ratio   10-30mSession访问时长占比
  * @param visit_length_30m_ratio       30mSession访问时长占比
  * @param step_length_1_3_ratio        1-3步长占比
  * @param step_length_4_6_ratio        4-6步长占比
  * @param step_length_7_9_ratio        7-9步长占比
  * @param step_length_10_30_ratio      10-30步长占比
  * @param step_length_30_60_ratio      30-60步长占比
  * @param step_length_60_ratio         大于60步长占比
  */
case class SessionAggrStat(taskid: String,
                           session_count: Long,
                           visit_length_1s_3s_ratio: Double,
                           visit_length_4s_6s_ratio: Double,
                           visit_length_7s_9s_ratio: Double,
                           visit_length_10s_30s_ratio: Double,
                           visit_length_30s_60s_ratio: Double,
                           visit_length_1m_3m_ratio: Double,
                           visit_length_3m_10m_ratio: Double,
                           visit_length_10m_30m_ratio: Double,
                           visit_length_30m_ratio: Double,
                           step_length_1_3_ratio: Double,
                           step_length_4_6_ratio: Double,
                           step_length_7_9_ratio: Double,
                           step_length_10_30_ratio: Double,
                           step_length_30_60_ratio: Double,
                           step_length_60_ratio: Double
                          )

/**
  * Session随机抽取表
  *
  * @param taskid             当前计算批次的ID
  * @param sessionid          抽取的Session的ID
  * @param startTime          Session的开始时间
  * @param searchKeywords     Session的查询字段
  * @param clickCategoryIds   Session点击的类别id集合
  */
case class SessionRandomExtract(taskid:String,
                                sessionid:String,
                                startTime:String,
                                searchKeywords:String,
                                clickCategoryIds:String)

/**
  * Session随机抽取详细表
  *
  * @param taskid            当前计算批次的ID
  * @param userid            用户的ID
  * @param sessionid         Session的ID
  * @param pageid            某个页面的ID
  * @param actionTime        点击行为的时间点
  * @param searchKeyword     用户搜索的关键词
  * @param clickCategoryId   某一个商品品类的ID
  * @param clickProductId    某一个商品的ID
  * @param orderCategoryIds  一次订单中所有品类的ID集合
  * @param orderProductIds   一次订单中所有商品的ID集合
  * @param payCategoryIds    一次支付中所有品类的ID集合
  * @param payProductIds     一次支付中所有商品的ID集合
  **/
case class SessionDetail(taskid:String,
                         userid:Long,
                         sessionid:String,
                         pageid:String,
                         actionTime:String,
                         searchKeyword:String,
                         clickCategoryId:String,
                         clickProductId:String,
                         orderCategoryIds:String,
                         orderProductIds:String,
                         payCategoryIds:String,
                         payProductIds:String)