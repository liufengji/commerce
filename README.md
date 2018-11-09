##电商分析

###项目架构
01--生成数据、session占比、session随机抽样
02--session-Top10、session-Top10活跃、page转化率

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
物品ID        物品名称          额外信息
product_id    product_name     extend_info

######动作表
动作日期   用户id     session_id   页面id     动作时间     查询词            点击类别id
date      user_id    session_id   page_id   action_time  search_keyword   click_category_id  

点击产品id          订单类别id集合        订单产品id集合      付款类别id集合      付款产品id集合
click_product_id   order_category_ids   order_product_ids  pay_category_ids   pay_product_ids

城市id
city_id


数据样例

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

