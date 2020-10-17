package com.aiguigu.apitest.tabletest.udftest

import com.aiguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    val inputPath = getClass.getResource("/sensor.txt").getPath
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 1. table api
    val top2TempAcc = new TopN()
    val resultTable = sensorTable
      .groupBy('id)
      .flatAggregate(top2TempAcc('temperature) as('temp, 'rank))
      .select('id, 'temp, 'rank)

    resultTable.toRetractStream[Row].print("result")

    env.execute()
  }
}

class Top2TempAcc {
  var highestTemp: Double = Double.MinValue
  var secondHighestTemp: Double = Double.MinValue
}

// 提取所有温度值中最高的两个温度 输出(temp, rank)
class TopN extends TableAggregateFunction[(Double, Int), Top2TempAcc] {

  // 实现计算聚合结果的函数accumulate
  def accumulate(acc: Top2TempAcc, temp: Double): Unit = {
    if (temp > acc.highestTemp) {
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if (temp > acc.secondHighestTemp) {
      acc.secondHighestTemp = temp
    }
  }

  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit = {
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp, 2))
  }

  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()
}


