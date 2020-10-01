package com.aiguigu.apitest

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

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

    // 4. 自定义Source
    val stream4 = env.addSource(new MySensorSource())
    stream4.print()

    env.execute("SourceTest")
  }
}

class MySensorSource extends SourceFunction[SensorReading] {

  // 标志位
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 随机数发生器
    val random = new Random()

    // 随机生成一组10个传感器初始温度
    var curTemp = 1.to(10).map( i => ("sensor_"+i, random.nextDouble() * 100))

    while (running) {
      // 再上次数据基础上微调更新温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + random.nextGaussian())
      )

      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )

      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
