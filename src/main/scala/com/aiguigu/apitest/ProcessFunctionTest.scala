package com.aiguigu.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

object ProcessFunctionTest {
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
    //      .keyBy(_.id)
    //      .process(new MyKeyedProcessFunction())

    // 需求 10s内温度一直上升则报警
    val warningStream = dataStream
      .keyBy(_.id)
      .process(new TempIncreaseWarning(10 * 1000))

    warningStream.print()

    env.execute("ProcessFunctionTest")
  }
}


class TempIncreaseWarning(interval: Long) extends KeyedProcessFunction[String, SensorReading, String] {

  // 定义状态: 保存上一个温度进行比较 保存注册定时器的时间戳用于删除定时器
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
  )

  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("timerTs", classOf[Long])
  )

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    // 先取出状态
    val lastTemp = lastTempState.value()
    val timerTs = timerTsState.value()
    lastTempState.update(value.temperature)

    if (value.temperature > lastTemp && timerTs == 0) {
      // 温度上升且没有定时器 那么注册当前数据时间戳10s后的计时器
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerEventTimeTimer(ts)
      timerTsState.update(ts)
    } else if (value.temperature < lastTemp) {
      // 温度下降 删除定时器
      ctx.timerService().deleteEventTimeTimer(timerTs)
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器" + ctx.getCurrentKey + "温度连续" + interval / 1000 + "秒内连续上升")
    timerTsState.clear()
  }
}

class MyKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, String] {

  var myState: ValueState[Int] = _

  override def processElement(v: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.getCurrentKey
    ctx.timestamp()
    ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60 * 1000)
    //    ctx.timerService().deleteEventTimeTimer()
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 定时器时间到的时候做的事情
  }

  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myState", classOf[Int]))
  }

  override def close(): Unit = {

  }
}