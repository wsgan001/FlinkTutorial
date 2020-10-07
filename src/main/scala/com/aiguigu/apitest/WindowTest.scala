package com.aiguigu.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = getClass.getResource("/sensor.txt").getPath
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // 每15秒统计一次各传感器温度的最小值, 以及最新的时间戳
    val resultStream = dataStream
      .map( data => (data.id, data.temperature, data.timestamp) )
      .keyBy(_._1) // 二元组的第一个元素
//      .window(TumblingEventTimeWindows.of(Time.seconds(15)))
//      .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8))) 注意时区问题
//      .timeWindow(Time.seconds(15)) // 和上面等价
      .timeWindow(Time.seconds(15)) // 本程序执行时间不足15秒 所以可以改为使用socketTextStrem
//      .minBy(1)
      .reduce((curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3))
//      .reduce(new MyReducer())

    resultStream.print()

    env.execute("WindowTest")
  }
}

class MyReducer extends ReduceFunction[SensorReading] {
  override def reduce(v1: SensorReading, v2: SensorReading): SensorReading = {
    SensorReading(v1.id, v2.timestamp, v1.temperature.min(v2.temperature))
  }
}
