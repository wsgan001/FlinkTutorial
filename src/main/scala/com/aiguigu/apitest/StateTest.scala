package com.aiguigu.apitest

import java.util

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.socketTextStream("node01", 7777)
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )

    // 需求: 对于温度传感器温度值跳变超过10度则报警

    val alertStream = dataStream
      .keyBy(_.id)
      //      .flatMap(new TempChangeAlert(10.0))
      .flatMapWithState[(String, Double, Double), Double]({
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp: Some[Double]) => {

          val diff = (data.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature))
          } else {
            (List.empty, Some(data.temperature))
          }
        }
      })
    alertStream.print()


    env.execute("StateTest")
  }
}

// 实现自定义RichFlatMapFunction

class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
  )

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值
    val diff = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      collector.collect((in.id, lastTemp, in.temperature))
    }
    lastTempState.update(in.temperature)
  }
}

// keyed state 测试 必须定义在RichFunction中 因为需要运行时上下文
class MyRichMapper extends RichMapFunction[SensorReading, String] {

  // 方式1: 提前声明
  var valueState: ValueState[Double] = _

  // 方式2: lazy方式
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(
    new ListStateDescriptor[Int]("listState", classOf[Int])
  )

  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double])
  )

  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(
    new ReducingStateDescriptor[SensorReading]("reducingState", new MyReducer(), classOf[SensorReading])
  )

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("valueState", classOf[Double])
    )
  }

  override def map(in: SensorReading): String = {
    // 状态的读写
    val myV = valueState.value()
    valueState.update(in.temperature)

    listState.add(1)
    listState.addAll(new util.ArrayList[Int]())
    //    listState.update()
    listState.get()


    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.2)
    mapState.keys()
    mapState.entries()
    mapState.remove("")

    // reducing state 调用一次reducing function
    reduceState.add(new SensorReading("sensor_1", 10000, 25.6))

    in.id
  }
}
