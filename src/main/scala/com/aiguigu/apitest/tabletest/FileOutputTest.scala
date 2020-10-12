package com.aiguigu.apitest.tabletest

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FileOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    val inputPath = getClass.getResource("/sensor.txt").getPath
    tableEnv.connect(new FileSystem().path(inputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    val sensorTable = tableEnv.from("inputTable")

    // 3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    // 不支持toAppendStream 而是调用toRetractStream，它返回二元组，前面的boolean表示是否失效了
    val aggTable = sensorTable
      .groupBy('id) // 基于id分组
      .select('id, 'id.count as 'count)

    aggTable.toRetractStream[(String, Long)].print()

    // 4. 输出到文件
    val outputPath = "out.txt"
    tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING)
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable")

    // 简单Table输出到文件
    resultTable.insertInto("outputTable")

    tableEnv.connect(new FileSystem().path(outputPath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable2")
    // 不支持这种方式 只能有插入的变化 不能有聚合的
    //    aggTable.insertInto("outputTable2")

    env.execute()
  }
}