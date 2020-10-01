package com.aiguigu.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * map flatMap filter 简单转换算子
 * keyBy rolling reduce 聚合转换算子 键控流转换算子
 * split select connect coMap union 多流转换算子
 */

object TransformTest {
  def main(args: Array[String]): Unit = {
    // 1. map 来一个转换为1个 1对1的转换
    // 2. flatMap 打散 1对多
    // 3. filter 给一个bool函数, 判断是否该元素要保留
    // 4. keyBy 定义两个任务间数据传输的模式 DataStream -> KeyedStream
    // 5. 滚动聚合算子: sum/min/max/minBy/maxBy
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    val inputPath = getClass.getResource("/sensor.txt").getPath
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream.map(
      data=>{
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
    // 分组聚合
    val aggStream = dataStream
      .keyBy("id") // 以id分组
      .minBy("temperature")

    // 输出当前最小温度值以及最近的时间戳
    val resultStream = dataStream
      .keyBy("id")
      .reduce((curState, newData) => {
        SensorReading(curState.id, newData.timestamp, curState.temperature.min(newData.temperature))
      })
//      .reduce(new MyReduceFunction())

    resultStream.print()
    env.execute("TransformTest")
  }
}

class MyReduceFunction extends ReduceFunction [SensorReading] {
  override def reduce(v1: SensorReading, v2: SensorReading): SensorReading = {
    SensorReading(v1.id, v2.timestamp, v1.temperature.min(v2.temperature))
  }
}
