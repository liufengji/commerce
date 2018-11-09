package com.victor.session

import org.apache.spark.util.AccumulatorV2

import scala.collection._

class SessionAggrStatAccumulator extends AccumulatorV2[String,mutable.HashMap[String,Int]]{

  private val aggrStatMap = mutable.HashMap[String,Int]()

  //isZero 判断是不是空
  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  //copy一个新的，把值给它
  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newacc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized{
      newacc.aggrStatMap ++= this.aggrStatMap
    }
    newacc
  }

  //clear()
  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  //add
  override def add(v: String): Unit = {
    if(!aggrStatMap.contains(v)){
      aggrStatMap += (v -> 0)
    }
    aggrStatMap.update(v,aggrStatMap(v) + 1)
  }

  //不同分区的合并
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc:SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value){ case (map,(k,v)) =>
          map += (k -> (v + map.getOrElse(k,0)))
        }

      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    aggrStatMap
  }
}
