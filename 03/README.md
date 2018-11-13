

Maven项目
commerce
    --mock  
        生产数据 -> Hive
        生产数据到 -> kafka

    --common //工具
        读取properties文件工具
        常量
        case 表 数据模型
        mysql 连接池
        util

    --analysis  //数据业务分析
      --session [spark core]  
         session分析,session聚合统计
         session随机抽取
         session Top10 热门品类
         session Top10 活跃session

      --advertising   [spark streaming]
        广告统计用户黑名单
        广告点击实时统计
        实时统计各省最热广告
        最近一小时广告点击趋势统计
        kafka offset

      --page  [spark core]
        页面单跳转化率分析

      --product   [spark sql]
        各区域热门商品统计分析,各区域Top3商品统计
        开窗函数