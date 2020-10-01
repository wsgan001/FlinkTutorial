package com.aiguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

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

    // 3. 从Kafka中读取数据
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "localhost:9092")
//    properties.setProperty("group.id", "consumer-group")
//    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
//    stream3.print()

    env.execute("SourceTest")
  }
}
