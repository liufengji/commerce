##电商分析

###项目架构

离线
data数据 -> Hive -> Spark Sql读取数据 -> Spark RDD  ETL 数据分析后 -> Spark Sql 把数据存入 mysql数据库

实时
data数据 -> kafka 集群 -> Spark Streamming ETL -> Mysql数据库

Maven项目
commerce
    --mock  //数据
    --common //工具
    --analysis  //数据业务分析
      --session  //session分析
      --advertising
      --page
      --product

###表信息

######用户表
用户id    用户名      名称   年龄   职业           城市    性别
user_id   username   name   age   professional   city    sex

######物品表
物品ID        product_name    extend_info
product_id    物品名称        额外信息

######动作表
date      user_id    session_id   page_id   action_time  search_keyword   click_category_id  
动作日期   用户id     session_id   页面id     动作时间     查询词            点击类别id

click_product_id   order_category_ids   order_product_ids  pay_category_ids   pay_product_ids
点击产品id          订单类别id集合        订单产品id集合      付款类别id集合      付款产品id集合

city_id
城市id


需求一:session聚合统计

UserVisitAction
2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,24,22,6
2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,34,45,6

UserInfo
88,user88,name88,31,profess77,0,female
81,user88,name88,31,profess77,2,female

ProductInfo
14,product14,0
18,product45,1

task.params.json={startDate:"2017-10-20", \
  endDate:"2017-10-31", \
  startAge: 20, \
  endAge: 50, \
  professionals: "",  \
  cities: "", \
  sex:"", \
  keywords:"", \
  categoryIds:"", \
  targetPageFlow:"1,2,3,4,5,6,7"}


（1）从mysql读取task.params.json

（2）Spark Sql ("select ..")  读取 hive 用户行为数据

（3）//将行为数据转换成K-V结构，sessionid为key
val sessionid2userVisitActionRDD = userVisitActionRDD.map(item => (item.session_id, item))
2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,24,22,6
                   ↓
3f7358c3-ad34-48j5-99c8-32cf2d78a06c
           -- 2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,24,22,6

（4）将Sessionid相同的数据进行聚合，计算出访问步长、访问时长, 将K转变为 userid
val sessionid2userVisitActionsRDD = sessionid2userVisitActionRDD.groupByKey()
3f7358c3-ad34-48j5-99c8-32cf2d78a06c
           -- 2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,24,22,6
3f7358c3-ad34-48j5-99c8-32cf2d78a06c
           -- 2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,24,22,6
                   ↓
3f7358c3-ad34-48j5-99c8-32cf2d78a06c
           -- 2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,24,22,6
           -- 2018-11-08,0,3f7358c3-ad34-48j5-99c8-32cf2d78a06c,9,2018-11-08 09:03:17,,,,,,24,22,6

（5）//从Hive中获取用户数据
    val userInfoDS = spark.sql("select * from "+Constants.TABLE_USER_INFO).as[UserInfo]val 
    val userInfoRDD = userInfoDS.rdd
    userid2userInfoRDD = userInfoRDD.map(item => (item.user_id, item))

（6） //将用户数据和聚合后的用户行为Session进行 JOIN，生成新的聚合数据   
 val sessionid2fullAggrInfo = userid2partAggrInfo.join(userid2userInfoRDD).map{ case (userid, (partAggrInfo, userInfo)) =>
      (partAggrInfo.session_id, FullAggrInfo(partAggrInfo,userInfo))
    }

（7） //将数据根据任务的配置信息进行过滤，在过滤的过程中更新累加器


（8）写入mysql