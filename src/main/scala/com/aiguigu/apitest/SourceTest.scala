package com.aiguigu.apitest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合中读取数据(有界流)
//    val dataList = List(
//      SensorReading("Sensor_1", 1547718199, 35.8),
//      SensorReading("Sensor_6", 1547718201, 15.4),
//      SensorReading("Sensor_7", 1547718202, 6.7),
//      SensorReading("Sensor_10", 1547718205, 38.1),
//    )
//    val stream1 = env.fromCollection(dataList)
//    stream1.print()

    // 2. 从文件读取(有界流)
//    val inputPath = getClass.getResource("/sensor.txt").getPath
//    val stream2 = env.readTextFile(inputPath)
//    stream2.print()

    env.execute("SourceTest")
  }
}
