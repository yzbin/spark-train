package com.robin

/**
  * 使用Spark Streaming完成有状态统计
  */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkconf,Seconds(5))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream("localhost",6789)
    val result = lines.flatMap(_.split(" ")).map((_,1))

    val state = result.updateStateByKey[Int](updateFunction _)



    result.print()

    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * 用当前的数据去更新老的数据
    * @param currentValues 当前的
    * @param preValues 老的
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    Some(current + pre)
  }

}
