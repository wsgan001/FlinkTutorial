package com.aiguigu.apitest.tabletest

import com.aiguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}

object Example {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputPath = getClass.getResource("/sensor.txt").getPath
    val inputStream = env.readTextFile(inputPath)
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val tableEnv = StreamTableEnvironment.create(env)

    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    val resultTable = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")
    resultTable.toAppendStream[(String, Double)]
        .print("TableApi")

    // 用sql实现
    tableEnv.createTemporaryView("dataTable", dataTable)
    val sql = "SELECT id,temperature FROM dataTable WHERE id='sensor_6'"
    val resultSqlTable = tableEnv.sqlQuery(sql)
    resultSqlTable.toAppendStream[(String, Double)].print("SQL")

    println(tableEnv.explain(resultSqlTable))

    env.execute("TableApiExample")
  }
}