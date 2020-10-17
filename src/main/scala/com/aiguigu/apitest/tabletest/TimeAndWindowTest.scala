package com.aiguigu.apitest.tabletest

import com.aiguigu.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.api.{EnvironmentSettings, Over, Tumble}
import org.apache.flink.types.Row

object TimeAndWindowTest {
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

    // 1. Group Window
    // 1. table api
    val resultTable = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw) // 每10s统计一次 滚动时间窗口
      .groupBy('id, 'tw)
      .select('id, 'id.count, 'temperature.avg, 'tw.end)

    // 2. sql
    tableEnv.createTemporaryView("sensor", sensorTable)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |SELECT
        |  id,
        |  count(id),
        |  AVG(temperature),
        |  tumble_end(ts, interval '10' second)
        |FROM sensor
        |GROUP BY
        |  id,
        |  tumble(ts, interval '10' second)
        |""".stripMargin)

    // 转换成流 打印输出
    // 因为没有迟到数据，结果不会改动，所以可以使用toAppendStream
    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toRetractStream[Row].print("sqlResult")

    val overResultTable = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id, 'ts, 'id.count over 'ow, 'temperature.avg over 'ow)
    overResultTable.toAppendStream[Row].print("overResult")

    // 2.2 sql
    val overResultSqlTable = tableEnv.sqlQuery(
      """
        |SELECT
        |  id,
        |  ts,
        |  COUNT(id) OVER ow,
        |  AVG(temperature) OVER ow
        |FROM sensor
        |WINDOW ow AS(
        |    PARTITION BY id
        |    ORDER BY ts
        |    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        |)
  """.stripMargin
    )
    overResultSqlTable.toRetractStream[Row].print("sql")

    env.execute()
  }
}